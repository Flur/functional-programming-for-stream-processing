package main

object EonetEventService {

  // first is long second is latitude
  def getEonetEventCoordinates(eonetEvent: EonetEventFromDB): List[List[Double]] = {
    eonetEvent.geometry match {
      case Some(a) => getGeometriesPoints(a)
      // todo return as option
      case None => List()
    }
  }

  def getGeometriesPoints(eonetGeometryFromDB: List[EonetGeometryFromDB]): List[List[Double]] = {
    eonetGeometryFromDB.flatMap(
      geometry => {
        geometry.`type` match {
          case "Point" => List(geometry.point.getOrElse(List()))
          case "Polygon" => geometry.polygon.getOrElse(List())
          case _ => List()
        }
      }
    )
  }

  // todo make trait for events and geometries
  def eonetEvetToEonetEventFromDB(eonetEvent: EonetEvent): EonetEventFromDB = {
    val newGeometryFromDb = eonetEvent.geometry.map(eg => {
      var point: List[Double] = List()
      var polygons: List[List[Double]] = List()

      eg.coordinates match {
        case Left(a) => point = a
        case Right(b) => polygons = b.flatten
      }

      EonetGeometryFromDB(0, eg.`type`, Option(point), Option(polygons), 0)
    })

    EonetEventFromDB(
      0, // bad solution, use trait as base
      eonetEvent.id,
      eonetEvent.title,
      eonetEvent.description,
      Option(eonetEvent.link),
      eonetEvent.closed,
      Option(newGeometryFromDb)
    )
  }

}