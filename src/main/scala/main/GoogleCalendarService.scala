package main

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.calendar.Calendar
import com.google.api.services.calendar.CalendarScopes
import com.google.api.services.calendar.model.{CalendarListEntry, Event, Events}
import java.io.FileInputStream
import java.util
import java.util.Collections

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

object GoogleCalendarService {
  implicit val system = ActorSystem()
  implicit val dispatcher = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val APPLICATION_NAME = "Google Calendar API"
  val JSON_FACTORY: JacksonFactory = JacksonFactory.getDefaultInstance
  val DISASTER_CALENDAR_ID = "52h4q6tp1o0sv44hu8jnuji7og@group.calendar.google.com"
  val SCOPES: util.List[String] = Collections.singletonList(CalendarScopes.CALENDAR_READONLY)
  // path to your json service account key
  val CREDENTIALS_FILE_PATH: String= "./src/main/resources/vivid-router-301320-f01a20c1352d.json"

  def getCalendar: Calendar = {
    val credential: GoogleCredential = GoogleCredential.fromStream(new FileInputStream(CREDENTIALS_FILE_PATH))
      .createScoped(SCOPES);

    val HTTP_TRANSPORT: NetHttpTransport = GoogleNetHttpTransport.newTrustedTransport()

    new Calendar.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
      .setApplicationName(APPLICATION_NAME)
      .build()
  }

  def getEventsList(calendarId: String): util.List[Event] = {
    val events: Events = getCalendar.events().list(calendarId)
          .execute()

    events.getItems
  }

  def getEventsForDisasterCalendar: util.List[Event] = {
    getEventsList(DISASTER_CALENDAR_ID)
  }

  def getCalendarList: List[CalendarListEntry] = {
    val calendarList = getCalendar.calendarList().list.execute()

    calendarList.getItems.asScala.toList
  }

  def getEventSource(): Source[Event, NotUsed] = {
    Source
      .repeat(getEventsForDisasterCalendar)
      .map(i => i.asScala.toList)
      .throttle(1, 30.seconds)
      .mapConcat(identity)
      // if event doesn't have location, then there is no sense to continue
      .filter(ev => Option(ev.getLocation).nonEmpty )
  }
}
