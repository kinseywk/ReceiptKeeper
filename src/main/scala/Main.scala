import java.io._
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.util.Random
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.mongodb.scala._
import org.mongodb.scala.result._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model._
import com.github.tototoshi.csv._

object Helpers {
  implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: (Document) => String = (doc) => doc.toJson
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: (C) => String = (doc) => Option(doc).map(_.toString).getOrElse("")
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: (C) => String

    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
    def headResult() = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))
    def printResults(initial: String = ""): Unit = {
      if (initial.length > 0) print(initial)
      results().foreach(res => println(converter(res)))
    }
    def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
  }
}

sealed trait VendorCategory
case object GeneralGoods extends VendorCategory
case object Grocery extends VendorCategory
case object Electronics extends VendorCategory
case object Gas extends VendorCategory
case object FastFood extends VendorCategory
case object Entertainment extends VendorCategory

case class Vendor(name: String, category: String, min: Float, max: Float)

sealed trait Period
case object Year extends Period
case object Month extends Period
case object Fortnight extends Period
case object Week extends Period
case object Day extends Period

case class Frequency(quantity: Int, per: Period)

case class Receipt(customer: String, timestamp: Long, vendor: String, description: String, amount: Float)

object Global {
  val database = "ReceiptKeeper"
  val collection = "receipts"
  val timestamp = 1600000000

  val vendors = Seq(
    Vendor("Amazon", "General Goods", 10.0f, 500.0f),
    Vendor("Wal-Mart", "General Goods", 25.0f, 350.0f),
    Vendor("Publix", "Grocery", 50.0f, 200.0f),
    Vendor("BP", "Gas", 20.0f, 50.0f),
    Vendor("Shell", "Gas", 20.0f, 50.0f),
    Vendor("Newegg", "Electronics", 50.0f, 1000.0f),
    Vendor("Burger King", "Fast Food", 5.0f, 10.0f),
    Vendor("McDonald's", "Fast Food", 5.0f, 10.0f),
    Vendor("Movie Theatre", "Entertainment", 30.0f, 60.0f),
    Vendor("Amusement Park", "Entertainment", 60.0f, 150.0f)
  )

  val purchases = Seq(
    ("Grocery", Frequency(1, Week)),
    ("General Goods", Frequency(1, Month)),
    ("Electronics", Frequency(2, Year)),
    ("Entertainment", Frequency(1, Month)),
    ("Gas", Frequency(1, Fortnight)),
    ("Fast Food", Frequency(1, Week))
  )

  val names = Seq(
    "Alice",
    "Bob",
    "Cheryl",
    "Dale",
    "Ed",
    "Frank",
    "Gary",
    "Hannah",
    "Ingrid",
    "Jojo",
    "Kelly",
    "Lilly",
    "Marvin",
    "Nancy",
    "Ovi",
    "Pam",
    "Quincy",
    "Robert",
    "Sally",
    "Terrance",
    "Uther",
    "Vinny",
    "Wendy",
    "Yvonne",
    "Zed"
  )

  def generateReceipt(): Receipt = {
    val vendor = vendors(Random.nextInt().abs % vendors.length);
    Receipt(
      names(Random.nextInt().abs % names.length),
      Random.nextInt(30000000) + timestamp,
      vendor.name,
      vendor.category,
      (Random.nextFloat() * (vendor.max - vendor.min)) + vendor.min
    )
  }
}

//Delete everything in the database
object NukeDatabase extends App {
  import Helpers._

  val client = MongoClient()
  val database = client.getDatabase(Global.database)
  val observable = database.drop().printResults()
}

object GenerateReceipt extends App {
  import Helpers._

  try {
    val client = MongoClient()
    val database = client.getDatabase(Global.database)
    val collection = database.getCollection(Global.collection)
    val receipt = Global.generateReceipt()
    val document = Document(
      "customer" -> receipt.customer,
      "timestamp" -> receipt.timestamp.toInt,
      "vendor" -> receipt.vendor,
      "description" -> receipt.description,
      "amount" -> receipt.amount.toDouble
    )

    collection.insertOne(document).results()
  } catch {
    case default: Throwable => println(s"$default")
  }
}

//Load a CSV file and insert its records into the database
object LoadCsv extends App {
  import Helpers._

  val reader = CSVReader.open(new File("Sample.csv"))
  
  reader.foreach(fields => {
    try {
      val receipt = Receipt(fields(0),
        fields(1).toLong,
        fields(2),
        fields(3),
        fields(4).toFloat
      )

      try {
        val client = MongoClient()
        val database = client.getDatabase(Global.database)
        val collection = database.getCollection(Global.collection)
        val document = Document(
          "customer" -> receipt.customer,
          "timestamp" -> receipt.timestamp.toInt,
          "vendor" -> receipt.vendor,
          "description" -> receipt.description,
          "amount" -> receipt.amount.toDouble
        )

        collection.insertOne(document).results()
      } catch {
        case default: Throwable => println(s"$default")
      }
    } catch {
      case _: Throwable => println("Malformed CSV")
    }
  })
  reader.close()
}

//Populate database with fake profiles full of random data
object PopulateDatabase extends App {
  import Helpers._

  try {
    val client = MongoClient()
    val database = client.getDatabase(Global.database)
    val collection = database.getCollection(Global.collection)

    for(a <- 1 to 1000) {
      val receipt = Global.generateReceipt()
      val document = Document(
        "customer" -> receipt.customer,
        "timestamp" -> receipt.timestamp.toInt,
        "vendor" -> receipt.vendor,
        "description" -> receipt.description,
        "amount" -> receipt.amount.toDouble
      )

      collection.insertOne(document).results()
    }
  } catch {
    case default: Throwable => println(s"$default")
  }
}

//Total amount spent on Amazon purchases
object TotalSpentAmazon extends App {
  import Helpers._

  try {
    val client = MongoClient()
    val database = client.getDatabase(Global.database)
    val collection = database.getCollection(Global.collection)

    collection.aggregate(Seq(
      Aggregates.filter(Filters.equal("vendor", "Amazon")),
      Aggregates.group("$vendor", Accumulators.sum("vendor", "$amount"))
    )).printResults()
  } catch {
    case default: Throwable => println(s"$default")
  }
}

//Who has the highest receipt total?
object GroupSpentMost extends App {
  import Helpers._

  try {
    val client = MongoClient()
    val database = client.getDatabase(Global.database)
    val collection = database.getCollection(Global.collection)

    collection.aggregate(Seq(
      Aggregates.group("$customer", Accumulators.sum("customer", "$amount"))
    )).printResults()
  } catch {
    case default: Throwable => println(s"$default")
  }
}

//What is the average receipt amount?
object AverageReceiptAmount extends App {
  import Helpers._

  try {
    val client = MongoClient()
    val database = client.getDatabase(Global.database)
    val collection = database.getCollection(Global.collection)

    collection.aggregate(Seq(
      Aggregates.group("$customer", Accumulators.avg("customer", "$amount"))
    )).printResults()
  } catch {
    case default: Throwable => println(s"$default")
  }
}

object IndividualReceiptCount extends App {
  import Helpers._

  try {
    val client = MongoClient()
    val database = client.getDatabase(Global.database)
    val collection = database.getCollection(Global.collection)

    collection.aggregate(Seq(
      Aggregates.group("$customer")
    )).printResults()
  } catch {
    case default: Throwable => println(s"$default")
  }
}

//What was a person's highest receipt?
object IndividualMostExpensive extends App {
  import Helpers._

  try {
    val client = MongoClient()
    val database = client.getDatabase(Global.database)
    val collection = database.getCollection(Global.collection)

    collection.aggregate(Seq(
      Aggregates.group("$customer", Accumulators.max("customer", "$amount"))
    )).results().foreach((foo) => println(foo))
  } catch {
    case default: Throwable => println(s"$default")
  }
}

//Who spent the most on groceries?
//object GroupSpentGroceries extends App {}

//How much does a person spend on Amazon per year?
//object IndividualAmazonPerYear extends App {}

//How much does a person spend each month?
//object IndividualMedianMonthlyTotal extends App {}