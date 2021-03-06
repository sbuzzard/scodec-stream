package scodec
package stream

import language.higherKinds

import scalaz.stream.{ Process, process1, Process1 }
import scalaz.stream.{ Process => P }
import scalaz.concurrent.Task
import scalaz.{ \/, -\/, \/-, Monad, MonadPlus }
import scodec.bits.BitVector

import shapeless.Lazy

/**
 * Module containing various streaming decoding combinators.
 * Decoding errors are represented using [[scodec.stream.decode.DecodingError]].
 */
package object decode {

  /** The decoder that consumes no input and emits no values. */
  val halt: StreamDecoder[Nothing] =
    StreamDecoder.instance { P.halt }

  /** The decoder that consumes no input and halts with the given error. */
  def fail(err: Throwable): StreamDecoder[Nothing] =
    StreamDecoder.instance { P.fail(err) }

  /** The decoder that consumes no input and halts with the given error. */
  def fail(err: Err): StreamDecoder[Nothing] =
    StreamDecoder.instance { P.fail(DecodingError(err)) }

  /** The decoder that consumes no input, emits the given `a`, then halts. */
  def emit[A](a: A): StreamDecoder[A] =
    StreamDecoder.instance { P.emit(a) }

  /** The decoder that consumes no input, emits the given `A` values, then halts. */
  def emitAll[A](as: Seq[A]): StreamDecoder[A] =
    StreamDecoder.instance { Process.emitAll(as) }

  /** Obtain the current input. This stream returns a single element. */
  def ask: StreamDecoder[BitVector] =
    StreamDecoder.instance { Process.eval(Cursor.ask) }

  /** Modify the current input. This stream returns a single element. */
  def modify(f: BitVector => BitVector): StreamDecoder[BitVector] =
    StreamDecoder.instance { Process.eval(Cursor.modify(f)) }

  /** Advance the input by the given number of bits. */
  def drop(n: Long): StreamDecoder[BitVector] =
    StreamDecoder.instance { Process.eval(Cursor.modify(_.drop(n))) }

  /** Advance the input by the given number of bits, purely as an effect. */
  def advance(n: Long): StreamDecoder[Nothing] =
    drop(n).edit(_.drain)

  /** Set the current cursor to the given `BitVector`. */
  def set(bits: BitVector): StreamDecoder[Nothing] =
    StreamDecoder.instance { Process.eval_(Cursor.set(bits)) }

  /** Trim the input by calling `take(n)` on the input `BitVector`. */
  def take(n: Long): StreamDecoder[BitVector] =
    StreamDecoder.instance { Process.eval(Cursor.modify(_.take(n))) }

  /** Produce a `StreamDecoder` lazily. */
  def suspend[A](d: => StreamDecoder[A]): StreamDecoder[A] =
    // a bit hacky - we are using an `ask` whose result is ignored
    // just to give us an `Await` to guard production of `d`
    ask.map(Box(_)).flatMap { bits => bits.clear(); d }

  /**
   * Run the given `StreamDecoder` using only the first `numberOfBits` bits of
   * the current stream, then advance the cursor by that many bits on completion.
   */
  def isolate[A](numberOfBits: Long)(d: => StreamDecoder[A]): StreamDecoder[A] =
    ask.map(Box(_)).flatMap { bits =>
      val rem = bits.get.drop(numberOfBits) // advance cursor right away
      bits.clear()                          // then clear the reference to bits to allow gc
      take(numberOfBits).edit(_.drain) ++ d ++ set(rem)
    }

  /**
   * Run the given `StreamDecoder` using only the first `numberOfBytes` bytes of
   * the current stream, then advance the cursor by that many bytes on completion.
   */
  def isolateBytes[A](numberOfBytes: Long)(d: => StreamDecoder[A]): StreamDecoder[A] =
    isolate(numberOfBits = numberOfBytes * 8)(d)

  /** Run the given `Decoder[A]` on `in`, returning its result as a `StreamDecoder`. */
  private[decode] def runDecode[A](in: BitVector)(implicit A: Lazy[Decoder[A]]): StreamDecoder[A] =
    A.value.decode(in).fold(
      fail,
      { result => set(result.remainder) ++ emit(result.value) }
    )

  /** Run the given `Decoder` once and emit its result, if successful. */
  def once[A](implicit A: Lazy[Decoder[A]]): StreamDecoder[A] =
    ask flatMap { runDecode[A] }

  /**
   * Like [[scodec.stream.decode.once]], but halts normally and leaves the
   * input unconsumed in the event of a decoding error. `tryOnce[A].repeat`
   * will produce the same result as `many[A]`, but allows.
   */
  def tryOnce[A](implicit A: Lazy[Decoder[A]]): StreamDecoder[A] = ask flatMap { in =>
    A.value.decode(in).fold(
      _ => halt,
      { result => set(result.remainder) ++ emit(result.value) }
    )
  }

  @annotation.tailrec
  private def consume[A](A: Decoder[A])(bits: BitVector, acc: Vector[A]):
      (BitVector, Vector[A], Err) =
    A.decode(bits) match {
      case Attempt.Failure(err) => (bits, acc, err)
      case Attempt.Successful(DecodeResult(a, rem)) => consume(A)(rem, acc :+ a)
    }

  /**
   * Promote a decoder to a `Process1`. The returned `Process1` may be
   * given chunks larger or smaller than what is needed to decode a single
   * element, and will buffer any unconsumed input, but a decoding error
   * will result if the decoder fails for a reason other than `Err.InsufficientBits`.
   *
   * This combinator relies on the decoder satisfying the following
   * property: If successful on input `x`, `A` should also succeed with the
   * same value given input `x ++ y`, for any choice of `y`. This ensures the
   * decoder can be given more input than it needs without affecting
   * correctness, and generally restricts this combinator to being used with
   * "self-delimited" decoders. Using this combinator with a decoder not
   * satisfying this property will make results highly dependent on the sequence
   * of chunk sizes passed to the process.
   */
  def process[A](implicit A: Lazy[Decoder[A]]): Process1[BitVector,A] = {
    def waiting(leftover: BitVector): Process1[BitVector,A] =
      P.await1[BitVector] flatMap { bits =>
        consume(A.value)(leftover ++ bits, Vector.empty) match {
          case (rem, out, err: Err.InsufficientBits) => P.emitAll(out) ++ waiting(rem)
          case (rem, Vector(), lastError) => P.fail(DecodingError(lastError))
          case (rem, out, _) => P.emitAll(out) ++ waiting(rem)
        }
      }
    waiting(BitVector.empty)
  }

  /**
   * Like [[scodec.stream.decode.many]], but in the event of a decoding error,
   * resets cursor to end of last successful decode, then halts normally.
   */
  def tryMany[A](implicit A: Lazy[Decoder[A]]): StreamDecoder[A] =
    tryOnce(A).map(Some(_)).or(emit(None)).flatMap {
      case None => halt
      case Some(a) => emit(a) ++ tryMany[A]
    }

  /**
   * Like `scodec.stream.decode.tryMany`, but reads up to `chunkSize` elements
   * at once. As mentioned in [[scodec.stream.decode.manyChunked]], the resulting
   * decoder cannot be meaningfully interleaved with other decoders.
   */
  def tryManyChunked[A](chunkSize: Int)(implicit A: Lazy[Decoder[A]]): StreamDecoder[A] =
    if (chunkSize < 1) throw new IllegalArgumentException("chunk size must be positive: " + chunkSize)
    else ask.map(Box(_)) flatMap { box =>
      var cur = box.get; box.clear()
      val buf = new collection.mutable.ArrayBuffer[A]
      try {
        while (cur.nonEmpty && buf.size < chunkSize) {
          A.value.decode(cur).fold(
            msg => throw new DecodingError(msg),
            { result => cur = result.remainder; buf += result.value }
          )
        }
        set(cur) ++ {
          if (buf.nonEmpty) StreamDecoder.instance { P.emitAll(buf) } ++ tryManyChunked(chunkSize)
          else halt
        }
      }
      catch { case e: DecodingError => halt }
    }

  /**
   * Runs `p1`, then runs `p2` if `p1` emits no elements.
   * Example: `or(tryOnce(codecs.int32), once(codecs.uint32))`.
   * This function does no backtracking of its own; backtracking
   * should be handled by `p1`.
   */
  def or[A](p1: StreamDecoder[A], p2: StreamDecoder[A]) =
    p1.edit { p1 => orImpl(p1, p2.decoder) }

  // generic combinator, this could be added to scalaz-stream
  private def orImpl[F[_],A](p: Process[F,A], p2: Process[F,A]): Process[F,A] = {
    // emit a single `None` if `p` is empty, otherwise wrap outputs in `Some`
    val terminated = p |> process1.awaitOption[A].flatMap {
      case None => P.emit(None)
      case Some(a) => process1.shiftRight(a).map(Some(_))
    }
    terminated.flatMap {
      case None => p2 // if there's a `None`, `p` must have been empty, switch to `p2`
      case Some(a) => P.emit(a) // if there's a `Some`, `p` nonempty, so ignore `p2`
    }
  }

  /**
   * Run this decoder, but leave its input unconsumed. Note that this
   * requires keeping the current stream in memory for as long as the
   * given decoder takes to complete.
   */
  def peek[A](d: StreamDecoder[A]): StreamDecoder[A] =
    ask flatMap { saved => d ++ set(saved) }

  /**
   * Like `many`, but reads up to `chunkSize` elements from the stream at a time,
   * to minimize overhead. Since this "reads ahead" in the stream, it can't be
   * used predictably when the returned decoder will be interleaved with another
   * decoder, as in `sepBy`, or any other combinator that uses `tee`.
   */
  def manyChunked[A](chunkSize: Int)(implicit A: Lazy[Decoder[A]]): StreamDecoder[A] =
    if (chunkSize < 1) throw new IllegalArgumentException("chunk size must be positive: " + chunkSize)
    else ask.map(Box(_)) flatMap { box =>
      var cur = box.get; box.clear()
      val buf = new collection.mutable.ArrayBuffer[A]
      while (cur.nonEmpty && buf.size < chunkSize) {
        A.value.decode(cur).fold(
          msg => throw new DecodingError(msg),
          { result => cur = result.remainder; buf += result.value }
        )
      }
      set(cur) ++ {
        if (buf.nonEmpty) StreamDecoder.instance { P.emitAll(buf) } ++ manyChunked(chunkSize)
        else halt
      }
    }

  /**
   * Parse a stream of `A` values from the input, using the given decoder.
   * The returned stream terminates normally if the final value decoded
   * exhausts `in` and leaves no trailing bits. The returned stream terminates
   * with an error if the `Decoder[A]` ever fails on the input.
   */
  def many[A](implicit A: Lazy[Decoder[A]]): StreamDecoder[A] =
    once[A].many

  /**
   * Like [[scodec.stream.decode.many]], but fails with an error if no
   * elements are decoded. The returned stream will have at least one
   * element if it succeeds.
   */
  def many1[A](implicit A: Lazy[Decoder[A]]): StreamDecoder[A] =
    many[A].nonEmpty(Err("many1 produced no outputs"))

  /**
   * Like `many`, but parses and ignores a `D` delimiter value in between
   * decoding each `A` value.
   */
  def sepBy[A, D](implicit A: Lazy[Decoder[A]], D: Lazy[Decoder[D]]): StreamDecoder[A] =
    once[A] flatMap { hd =>
      emit(hd) ++ many(D.value ~ A.value).map(_._2)
    }

  /**
   * Like `[[scodec.stream.decode.sepBy]]`, but fails with an error if no
   * elements are decoded. The returned stream will have at least one
   * element if it succeeds.
   */
  def sepBy1[A, D](implicit A: Lazy[Decoder[A]], D: Lazy[Decoder[D]]): StreamDecoder[A] =
    sepBy[A, D].nonEmpty(Err("sepBy1 given empty input"))

  private implicit class DecoderSyntax[A](A: Decoder[A]) {
    def ~[B](B: Decoder[B]): Decoder[(A,B)] = new Decoder[(A,B)] {
      def decode(bits: BitVector) = Decoder.decodeBoth(A,B)(bits)
    }
  }
}

// mutable reference that we can null out for GC purposes
private[stream] case class Box[A>:Null](var get: A) {
  def clear(): Unit = get = null
}
