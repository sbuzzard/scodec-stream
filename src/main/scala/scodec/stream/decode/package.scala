package scodec
package stream

import scalaz.stream.{Process,process1}
import scalaz.stream.{Process => P, Tee}
import scalaz.concurrent.Task
import scalaz.{\/,~>,Monad,MonadPlus}
import scodec.bits.BitVector

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

  /** The decoder that consumes no input and halts with the given error message. */
  def fail(msg: String): StreamDecoder[Nothing] =
    StreamDecoder.instance { P.fail(DecodingError(msg)) }

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
  private[decode] def runDecode[A](in: BitVector)(implicit A: Decoder[A]): StreamDecoder[A] =
    A.decode(in).fold(
      fail,
      { case (rem,a) => set(rem) ++ emit(a) }
    )

  /** Run the given `Decoder` once and emit its result, if successful. */
  def once[A](implicit A: Decoder[A]): StreamDecoder[A] =
    ask flatMap { runDecode[A] }

  /** Add the given bits to the front of the input vector. */
  def prepend(bits: BitVector): StreamDecoder[Nothing] =
    // we use the constructor to avoid forcing the stream
    modify(BitVector.Append(bits, _)).edit { _.drain }

  /**
   * Returns the stream of (header, frame payload) pairs extracted
   * from a stream of frames. Each frame consists of a header,
   * decoded using `header`, from which we extract a size
   * in bits using the `sizeInBits` function. Folllowing the header
   * is a frame payload of the number of bits specified in the header.
   */
  def frames[H](header: StreamDecoder[H])(sizeInBits: H => Long): StreamDecoder[(H,BitVector)] =
    header flatMap { hdr =>
      ask flatMap { bits =>
        val actualBits = sizeInBits(hdr)
        emit(hdr -> bits.take(actualBits)) ++
        advance(actualBits) ++
        frames(header)(sizeInBits)
      }
    }

  /** Defined as: `frames(header)(sizeInBits).map(_._2)` */
  def framePayloads[H](header: StreamDecoder[H])(sizeInBits: H => Long): StreamDecoder[BitVector] =
    frames(header)(sizeInBits).map(_._2)

  /** Defined as: `prefixFrames(header)(sizeInBits).map(_._2)` */
  def prefixFramePayloads[H](header: StreamDecoder[H])(sizeInBits: H => Long): StreamDecoder[BitVector] =
    prefixFrames(header)(sizeInBits).map(_._2)

  /** Like `frames`, but frame payload sizes are specified in bytes. */
  def byteFrames[H](header: StreamDecoder[H])(sizeInBytes: H => Long): StreamDecoder[(H,BitVector)] =
    frames(header)(sizeInBytes andThen (_ * 8))

  /** Defined as: `prefixByteFrames(header)(sizeInBytes).map(_._2)` */
  def prefixByteFramePayloads[H](header: StreamDecoder[H])(sizeInBytes: H => Long): StreamDecoder[BitVector] =
    prefixByteFrames(header)(sizeInBytes).map(_._2)

  /**
   * Like `frames`, but each emitted frame payload is concatenated with
   * all previous frame payloads. Useful when constructing streaming
   * decoders that may need to straddle frame boundaries. For instance,
   * `prefixFrames(uint64)(identity).firstAfter(_.size < 32)` combines frames
   * until their combined size exceeds 32, then emits this accumulated
   * frame.
   */
  def prefixFrames[H](header: StreamDecoder[H])(sizeInBits: H => Long):
      StreamDecoder[(Vector[H], BitVector)] =
    frames(header)(sizeInBits) |>
    process1.scan(Vector[H]() -> BitVector.empty)((acc, frame: (H,BitVector)) =>
      (acc._1 :+ frame._1, acc._2 ++ frame._2)
    ).drop(1)

  /**
   * Like `prefixFrames`, but takes a function which extracts a size in
   * bytes from the header.
   */
  def prefixByteFrames[H](header: StreamDecoder[H])(sizeInBytes: H => Long):
      StreamDecoder[(Vector[H], BitVector)] =
    prefixFrames(header)(sizeInBytes andThen (_ * 8))

  /**
   * Run `d` on a stream of frames, where the frame size in bytes is parsed using
   * the given decoder. A frame of size less than 0 terminates decoding.
   */
  def isolateByteFrames[A](sizeInBytes: Decoder[Long])(d: StreamDecoder[A]): StreamDecoder[A] =
    isolateFrames(sizeInBytes.map(_ * 8))(d)

  /**
   * Run `d` on a stream of frames, where the frame size in bits is parsed using
   * the given decoder. A frame of size less than 0 terminates decoding.
   */
  def isolateFrames[A](sizeInBits: Decoder[Long])(d: StreamDecoder[A]): StreamDecoder[A] =
    once(sizeInBits) flatMap {
      case i if i < 0 => halt
      case actualBits => d.isolate(actualBits) ++ isolateFrames(sizeInBits)(d)
    }

  /**
   * Like [[scodec.stream.decode.once]], but halts normally and leaves the
   * input unconsumed in the event of a decoding error. `tryOnce[A].repeat`
   * will produce the same result as `many[A]`, but if decoding fails after
   * three elements have been read, the cursor is reset to just after the
   * end of the third element and this decoder halts without an error.
   */
  def tryOnce[A](implicit A: Decoder[A]): StreamDecoder[A] = ask flatMap { in =>
    A.decode(in).fold(
      _ => halt,
      { case (rem,a) => set(rem) ++ emit(a) }
    )
  }

  /**
   * Like [[scodec.stream.decode.many]], but in the event of a decoding error,
   * resets cursor to end of last successful decode, then halts normally.
   */
  def tryMany[A](implicit A: Decoder[A]): StreamDecoder[A] =
    tryOnce(A).map(Some(_)).or(emit(None)).flatMap {
      case None => halt
      case Some(a) => emit(a) ++ tryMany[A]
    }

  /**
   * Runs `p1`, then runs `p2` if `p1` emits no elements.
   * Example: `or(tryOnce(codecs.int32), once(codecs.uint32))`.
   * This function does no backtracking of its own; backtracking
   * should be handled by `p1`.
   */
  def or[A](p1: StreamDecoder[A], p2: StreamDecoder[A]) =
    p1.tee(p2)((P.awaitL[A].repeat: Tee[A,A,A]) orElse P.awaitR[A].repeat)

  /**
   * Parse a stream of `A` values from the input, using the given decoder.
   * The returned stream terminates normally if the final value decoded
   * exhausts `in` and leaves no trailing bits. The returned stream terminates
   * with an error if the `Decoder[A]` ever fails on the input.
   */
  def many[A](implicit A: Decoder[A]): StreamDecoder[A] =
    once[A].many

  /**
   * Like [[scodec.stream.decode.many]], but fails with an error if no
   * elements are decoded. The returned stream will have at least one
   * element if it succeeds.
   */
  def many1[A:Decoder]: StreamDecoder[A] =
    many[A].nonEmpty("many1 produced no outputs")

  /**
   * Like `many`, but parses and ignores a `D` delimiter value in between
   * decoding each `A` value.
   */
  def sepBy[A:Decoder,D:Decoder]: StreamDecoder[A] =
    once[A] flatMap { hd =>
      emit(hd) ++ many(Decoder[D] ~ Decoder[A]).map(_._2)
    }

  /**
   * Like `[[scodec.stream.decode.sepBy]]`, but fails with an error if no
   * elements are decoded. The returned stream will have at least one
   * element if it succeeds.
   */
  def sepBy1[A:Decoder,D:Decoder]: StreamDecoder[A] =
    sepBy[A,D].nonEmpty("sepBy1 given empty input")

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
