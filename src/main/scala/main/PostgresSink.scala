package main

import doobie._
import doobie.implicits._

import cats._
import cats.effect._
import cats.implicits._

case class EonetEvent(id: Int, bk_id: String, title: String, description: String, link: String)



object PostgresSink {
  import doobie.util.ExecutionContexts
  implicit val cs = IO.contextShift(ExecutionContexts.synchronous)



  val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",     // driver classname
    "jdbc:postgresql://localhost:5433/functional_programming",     // connect URL (driver-specific)
    "admin",                  // user
    "1111",                          // password
    Blocker.liftExecutionContext(ExecutionContexts.synchronous) // just for testing
  )

  // for .quick
  val y = xa.yolo
  import y._

  // todo create table
//  val drop =
//    sql"""
//    DROP TABLE IF EXISTS person
//  """.update.run
//
//  val create =
//    sql"""
//    CREATE TABLE person (
//      id   SERIAL,
//      name VARCHAR NOT NULL UNIQUE,
//      age  SMALLINT
//    )
//  """.update.run

  def apply(): Unit = {

    // We need a ContextShift[IO] before we can construct a Transactor[IO]. The passed ExecutionContext
    // is where nonblocking operations will be executed. For testing here we're using a synchronous EC.

    val program1 = 42.pure[ConnectionIO]
    // A transactor that gets connections from java.sql.DriverManager and executes blocking operations
    // on an our synchronous EC. See the chapter on connection handling for more info.


    val io = program1.transact(xa)
    // io: IO[Int] = Async(
    //   cats.effect.internals.IOBracket$$$Lambda$10675/1668231756@3e0db56b,
    //   false
    // )
    println(io.unsafeRunSync)

    val program2 = sql"select 42".query[Int].unique
    // program2: ConnectionIO[Int] = Suspend(
    //   BracketCase(
    //     Suspend(PrepareStatement("select 42")),
    //     doobie.hi.connection$$$Lambda$10667/1091598834@47674e04,
    //     cats.effect.Bracket$$Lambda$10669/460742804@43ea2e04
    //   )
    // )
    val io2 = program2.transact(xa)
    // io2: IO[Int] = Async(
    //   cats.effect.internals.IOBracket$$$Lambda$10675/1668231756@160f5296,
    //   false
    // )
    println(io2.unsafeRunSync)
    // res1: Int = 42
    // res0: Int = 42


    // applicative functor
    val program3a = {
      val a: ConnectionIO[Int] = sql"select 42".query[Int].unique
      val b: ConnectionIO[Double] = sql"select random()".query[Double].unique
      (a, b).tupled
    }

    println(program3a.transact(xa).unsafeRunSync)

    val res = sql"select * from eonet_event"
      .query[String]    // Query0[String]
      .to[List]         // ConnectionIO[List[String]]
      .quick
      .unsafeRunSync
//      .transact(xa)     // IO[List[String]]
//      .unsafeRunSync    // List[String]
//      .take(5)          // List[String]
//      .foreach(println) // Unit

    insert1("bk_id", "title", "description", "link").quick.unsafeRunSync

    allDisasters().quick.unsafeRunSync



//    Write[Country]
  }

  def insertDisaster(): Unit = {

  }

  def selectDisaster(): Unit = {

  }

  def allDisasters() = sql"""
      select *
      from eonet_event
    """.query[EonetEvent]

  def insert1(bk_id: String, title: String, description: String, link: String): Update0 =
    sql"insert into eonet_event (bk_id, title, description, link) values ($bk_id, $title, $description, $link)".update
}
