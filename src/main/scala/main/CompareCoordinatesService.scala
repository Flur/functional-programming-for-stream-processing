package main

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.google.api.services.calendar.model.Event

object CompareCoordinatesService {

  // todo could be used flow with context for GEvent
  def getFlow(): Flow[
    (List[EonetEventFromDB], NominatimResponse, Event),
    (List[(EonetEventFromDB, List[NominatimGeoJSON])], Event),
    NotUsed
  ] = Flow[(List[EonetEventFromDB], NominatimResponse, Event)]
    .map[(List[(EonetEventFromDB, List[NominatimGeoJSON])], Event)](
      t => {

        println("inside flow")

        val (eonetEvents, nominatimResponse, googleCalendarEvent) = t

        (
          eonetEvents
            .map{ee => CompareCoordinatesService.compareEonetEventLocationWithNominatim(ee, nominatimResponse) }
            .filter(_._2.nonEmpty),
          googleCalendarEvent
        )
      }
    )
    .filter(_._1.nonEmpty)

  def compareEonetEventLocationWithNominatim(
                                              eonetEventFromDB: EonetEventFromDB,
                                              nominatimResponse: NominatimResponse
                                            ): (EonetEventFromDB, List[NominatimGeoJSON]) = {
    val eonetEventCoordinates: List[List[Double]] = EonetEventService.getEonetEventCoordinates(eonetEventFromDB)
    var nominatimLocationsNearEvent: List[NominatimGeoJSON] = List()

    eonetEventCoordinates.foreach( (eventCoordinate: List[Double]) => {
      nominatimLocationsNearEvent = nominatimResponse.features.filter( (nominatimGeoJson: NominatimGeoJSON) => isNominatimCloseToEvent(nominatimGeoJson, eventCoordinate))
    })

    (eonetEventFromDB, nominatimLocationsNearEvent)
  }

  def isNominatimCloseToEvent(nominatimGeoJson: NominatimGeoJSON, eventCoordinate: List[Double]): Boolean = {
    val nominatimLocationCoordinates = NominatimService.getNominatimGeometryCoordinates(nominatimGeoJson)

    val nominatimLocationsCloseToEvent: Option[List[Double]] = nominatimLocationCoordinates.find(nominatimCoordinates => {
      val distanceBetweenTwoPoints = Haversine.apply(eventCoordinate.head, eventCoordinate.lift(1).get, nominatimCoordinates.head, nominatimCoordinates.lift(1).get)

      if ((distanceBetweenTwoPoints / 1000) < 30) {
        println(nominatimGeoJson)
        println(eventCoordinate)
      }

      // todo distance as const
      (distanceBetweenTwoPoints / 1000) < 30
    })

    nominatimLocationsCloseToEvent match {
      case Some(a) => true
      case None => false
    }
  }
}
