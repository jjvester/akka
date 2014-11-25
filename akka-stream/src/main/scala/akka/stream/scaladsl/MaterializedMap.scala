/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

/**
 * Returned by [[RunnableFlow#run]] and [[FlowGraph#run]] and can be used to retrieve the materialized
 * `Source` inputs or `Sink` outputs, e.g. [[SubscriberSource]] or [[PublisherSink]].
 */
trait MaterializedMap {

  /**
   * Retrieve a materialized `Source`, e.g. the `Subscriber` of a [[SubscriberSource]].
   */
  def get(key: Source[_]): key.MaterializedType

  /**
   * Retrieve a materialized `Sink`, e.g. the `Publisher` of a [[PublisherSink]].
   */
  def get(key: Sink[_]): key.MaterializedType

  /**
   * Retrieve a materialized `Key`.
   */
  def get(key: Key): key.MaterializedType

  /**
   * Merge two materialized maps.
   */
  def merge(otherMap: MaterializedMap): MaterializedMap

  /**
   * Update the materialized map with a new value.
   */
  def updated(key: AnyRef, value: Any): MaterializedMap

  /**
   * Check if this map is empty.
   */
  def isEmpty: Boolean

  /**
   * An iterator over the key value pairs in this materialized map.
   */
  def iterator: Iterator[(AnyRef, Any)]
}

object MaterializedMap extends {
  private val emptyInstance = MaterializedMapImpl(Map.empty)

  def empty: MaterializedMap = emptyInstance
}

/**
 * A key that is not directly tied to a sink or source instance.
 *
 * FIXME #16380 Clean up the overlap between Keys/Sinks/Sources
 */
trait Key {
  type MaterializedType

  /**
   * Materialize the value for this key. All Sink and Source keys have been materialized and exist in the map.
   */
  def materialize(map: MaterializedMap): MaterializedType
}

private[stream] case class MaterializedMapImpl(map: Map[AnyRef, Any]) extends MaterializedMap {
  private def failure(keyType: String, key: AnyRef) = new IllegalArgumentException(s"$keyType [$key] doesn't exist in this flow")

  override def get(key: Source[_]): key.MaterializedType = key match {
    case _: KeyedSource[_] ⇒ map.getOrElse(key, throw failure("Source", key)).asInstanceOf[key.MaterializedType]
    case _                 ⇒ ().asInstanceOf[key.MaterializedType]
  }

  override def get(key: Sink[_]): key.MaterializedType = key match {
    case _: KeyedSink[_] ⇒ map.getOrElse(key, throw failure("Sink", key)).asInstanceOf[key.MaterializedType]
    case _               ⇒ ().asInstanceOf[key.MaterializedType]
  }

  override def get(key: Key): key.MaterializedType = map.getOrElse(key, throw failure("Key", key)).asInstanceOf[key.MaterializedType]

  override def merge(otherMap: MaterializedMap) = {
    val result = if (map.isEmpty) otherMap
    else if (otherMap.isEmpty) this
    else MaterializedMapImpl(map ++ otherMap.iterator)
    result
  }

  override def updated(key: AnyRef, value: Any) = MaterializedMapImpl(map.updated(key, value))

  override def isEmpty = map.isEmpty

  override def iterator = map.iterator
}
