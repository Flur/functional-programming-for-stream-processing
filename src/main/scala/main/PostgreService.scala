package main

import akka.NotUsed
import akka.stream.scaladsl.{Flow}
import cats.data.NonEmptyList
import main.HttpServer.system
import doobie._
import doobie.implicits._
import cats.effect._
import cats.implicits._

import scala.concurrent.Future


case class EonetResponseDB(title: String, description: String, link: String)

object PostgresService {
  implicit val executionContext = system.executionContext
  implicit val cs = IO.contextShift(system.executionContext)

  implicit val eonetGeometryWrite: Write[EonetGeometry] =
    Write[(Option[Int], Option[String], String, String, Option[String], Option[String])].contramap(eg => {
      var point: Option[String] = None
      var polygon: Option[String] = None

      eg.coordinates match {
        case Left(a) => point = Option(a.mkString(", "))
        case Right(b) => polygon = Option(b.mkString(", "))
      }

      (eg.magnitudeValue, eg.magnitudeUnit, eg.date, eg.`type`, point, polygon)
    })

  val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",                                     // driver classname
    "jdbc:postgresql://localhost:5433/functional_programming",     // connect URL (driver-specific)
    "admin",                                                      // user
    "1111",                                                       // password
//    import doobie.util.ExecutionContexts
//    Blocker.liftExecutionContext(ExecutionContexts.synchronous)         // just for testing
    Blocker.liftExecutionContext(system.executionContext)
  )

  // for .quick
  //  val y = xa.yolo
  //  import y._

  def createTables(): Unit = {
    val dropEonetEvent =
      sql"""
           DROP TABLE IF EXISTS eonet_event, eonet_geometry, eonet_category, eonet_source,
           eonet_event_eonet_source_realtion, eonet_event_eonet_category_relation CASCADE
      """.update.run

    val createEonetEvent = sql"""
      CREATE TABLE IF NOT EXISTS eonet_event (
	      event_id SERIAL PRIMARY KEY,
	      eonet_event_id varchar(255) NOT NULL,
        title varchar(255),
        closed BOOLEAN,
        description varchar(255),
        link varchar(255),
        UNIQUE(eonet_event_id)
      )
    """.update.run

    // date should be timestamp
    val createEonetGeometry = sql"""
      CREATE TABLE IF NOT EXISTS eonet_geometry (
	      geometry_id SERIAL PRIMARY KEY,
	      magnitudeValue NUMERIC,
        magnitudeUnit varchar(255),
        date varchar(255),
        type varchar(255),
        point point,
        polygon polygon,
        event_id INTEGER,
        CONSTRAINT fk_eonet_event
          FOREIGN KEY(event_id)
	        REFERENCES eonet_event(event_id)
      )
    """.update.run

    val createEonetCategory = sql"""
      CREATE TABLE IF NOT EXISTS eonet_category (
	      category_id SERIAL PRIMARY KEY,
        eonet_category_id varchar(255),
        title varchar(255),
        UNIQUE (eonet_category_id)
      )
    """.update.run

    val createEonetSource = sql"""
      CREATE TABLE IF NOT EXISTS eonet_source (
	      source_id SERIAL PRIMARY KEY,
	      eonet_source_id varchar(255),
        url varchar(255),
        UNIQUE(eonet_source_id)
      )
    """.update.run

    val eventSourceRelation = sql"""
      CREATE TABLE IF NOT EXISTS eonet_event_eonet_source_relation (
        id SERIAL PRIMARY KEY,
        eonet_event_id INTEGER NOT NULL,
        eonet_source_id INTEGER NOT NULL,
        FOREIGN KEY (eonet_event_id) REFERENCES eonet_event(event_id),
        FOREIGN KEY (eonet_source_id) REFERENCES eonet_source(source_id),
        UNIQUE (eonet_event_id, eonet_source_id)
    )""".update.run

    val eventCategoryRelation = sql"""
      CREATE TABLE IF NOT EXISTS eonet_event_eonet_category_relation (
        id SERIAL PRIMARY KEY,
        eonet_event_id INTEGER NOT NULL,
        eonet_category_id INTEGER NOT NULL,
        FOREIGN KEY (eonet_category_id) REFERENCES eonet_category(category_id),
        FOREIGN KEY (eonet_event_id) REFERENCES eonet_event(event_id),
        UNIQUE (eonet_event_id, eonet_category_id)
    )""".update.run

    (
      dropEonetEvent,
      createEonetEvent,
      createEonetGeometry,
      createEonetCategory,
      createEonetSource,
      eventSourceRelation,
      eventCategoryRelation
    ).mapN(_ + _ + _ + _ + _ + _ + _).transact(xa).unsafeRunSync
  }

  def apply(): Unit = {
    createTables()
  }

  def getFlow: Flow[EonetEvent, Option[EonetEvent], NotUsed] = {
    Flow[EonetEvent].mapAsync(1) { eonetEvent => insertEonetEvent(eonetEvent) }
  }

  def insertEonetEvent(eonetEvent: EonetEvent): Future[Option[EonetEvent]] = {
    insertInEonetEvent(eonetEvent)
      .transact(xa)
      .unsafeToFuture
      .flatMap(r => {
        var eonetEventOpt = Option(eonetEvent)

        // 0 if this event already exists in db
        if (r == 0) {
          eonetEventOpt = Option.empty[EonetEvent]
        }

        inserEonetEventSourcesCategories(eonetEvent).transact(xa).unsafeToFuture()
        insertGeometryAndRelations(eonetEvent).transact(xa).unsafeToFuture()

        Future.successful(eonetEventOpt)
      })
  }

  def insertGeometryAndRelations(eonetEvent: EonetEvent): ConnectionIO[Unit] = {
    for {
      eventId <- selectEonetEventId(eonetEvent.id)
      categoriesIds <- selectCategoriesIds(eonetEvent.categories)
      sourcesIds <- selectSourcesIds(eonetEvent.sources)
      _ <- insertEonetGeometries(eonetEvent.geometry, eventId)
      _ <- insertInEventSourceRelations(sourcesIds, eventId)
      _ <- insertInEventCategoryRelations(categoriesIds, eventId)
    } yield ()
  }

  def inserEonetEventSourcesCategories(eonetEvent: EonetEvent): ConnectionIO[Unit] = {
    for {
      _ <- insertEonetSources(eonetEvent.sources)
      _ <- insertEonetCategories(eonetEvent.categories)
    } yield ()
  }

  def insertInEventSourceRelations(sourcesIds: List[Int], eventId: Int): ConnectionIO[Int] = {
    val sql = "INSERT INTO eonet_event_eonet_source_relation (eonet_source_id, eonet_event_id) " +
      s"VALUES (?, $eventId) ON CONFLICT DO NOTHING"

    Update[Int](sql).updateMany(sourcesIds)
  }

  def insertInEventCategoryRelations(categoriesIds: List[Int], eventId: Int): ConnectionIO[Int] = {
    val sql = "INSERT INTO eonet_event_eonet_category_relation (eonet_category_id, eonet_event_id) " +
      s"VALUES (?, $eventId) ON CONFLICT DO NOTHING"

    Update[Int](sql).updateMany(categoriesIds)
  }

  def insertInEonetEvent(eonetEvent: EonetEvent): ConnectionIO[Int] = {
    sql"""
      INSERT INTO eonet_event as ee (eonet_event_id, title, description, closed, link)
      VALUES (${eonetEvent.id}, ${eonetEvent.title}, ${eonetEvent.description}, ${eonetEvent.closed},
        ${eonetEvent.link})
      ON CONFLICT (eonet_event_id) DO UPDATE
        SET closed = EXCLUDED.closed
        WHERE ee.closed IS DISTINCT FROM EXCLUDED.closed
    """.stripMargin.update.run
  }

  def insertEonetGeometries(eonetGeometries: List[EonetGeometry], eventId: Int): ConnectionIO[Int] = {
    val sql = "INSERT INTO eonet_geometry (magnitudeValue, magnitudeUnit, date, type, point, polygon, event_id) " +
      s"VALUES (?, ?, ?, ?, point(?), polygon(?), $eventId) ON CONFLICT DO NOTHING"

    Update[EonetGeometry](sql).updateMany(eonetGeometries)
  }

  def insertEonetSources(eonetSources: List[EonetSource]): ConnectionIO[Int] = {
    val sql = s"INSERT INTO eonet_source (eonet_source_id, url) " +
      s"VALUES (?, ?) ON CONFLICT DO NOTHING"

    Update[EonetSource](sql).updateMany(eonetSources)
  }

  def insertEonetCategories(eonetCategories: List[EonetCategory]): ConnectionIO[Int] = {
    val sql = "INSERT INTO eonet_category (eonet_category_id, title) VALUES (?, ?) ON CONFLICT DO NOTHING"

    Update[EonetCategory](sql).updateMany(eonetCategories)
  }

  /**
   * Selects
   *
  **/

  def selectEonetEventId(eonetEventId: String): ConnectionIO[Int] = {
    sql"""
         SELECT event_id FROM eonet_event WHERE eonet_event_id = ${eonetEventId}
       """.stripMargin.query[Int].unique
  }

  def selectSourcesIds(eonetSources: List[EonetSource]): ConnectionIO[List[Int]] = {
    val list: Option[NonEmptyList[String]] = NonEmptyList.fromList(eonetSources.map(c => c.id))

    val sql = fr"""SELECT source_id FROM eonet_source WHERE """ ++ Fragments.in(fr"eonet_source_id", list.get)

    sql.query[Int].to[List]
  }

  def selectCategoriesIds(eonetCategories: List[EonetCategory]): ConnectionIO[List[Int]] = {
    val list: Option[NonEmptyList[String]] = NonEmptyList.fromList(eonetCategories.map(c => c.id))

    val sql = fr"""SELECT category_id FROM eonet_category WHERE """ ++ Fragments.in(fr"eonet_category_id", list.get)

    sql.query[Int].to[List]
  }
}

