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
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;

object GoogleCalendarService {
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

  def getCalendarList: util.List[CalendarListEntry] = {
    val calendarList = getCalendar.calendarList().list.execute()

    calendarList.getItems
  }
}
