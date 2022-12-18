package aggregateService

import akka.actor.typed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{post, _}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.io.StdIn
import scala.language.{existentials, postfixOps}
import scala.util.{Failure, Success}
import akka.stream.OverflowStrategy
import com.typesafe.config.ConfigFactory

object Service {

  val config = ConfigFactory.load()
  var limit = config.getConfig("appConfig").getInt("limit")

  private val actorRefs = mutable.Map.empty[String, ActorRef[Message]]

  sealed trait Message
  case class ErrorMessage(error: String) extends Message
  case class SuccessMessage(msg: String) extends Message
  case class OfferApi(price: Int, productCode: String) extends Message
  case class Offer(replyTo: ActorRef[Message],price: Int, productCode: String) extends Message
  case class GetAggregateOffersByProduct(replyTo: ActorRef[OfferAggregationReply],productCode: String) extends Message
  case class OfferAggregationReply( offerAggregation: OfferAggregation) extends Message
  case class OfferAggregation(min: Int, max: Int, avg: Int, count: Int)
  case class OfferAggregationActorState(status: String,offerAggregationStatus: OfferAggregation)
  case class Close(repl: ActorRef[Message], productCode: String) extends Message


  object OfferAggregationActor{

    def addAggregation(state: OfferAggregationActorState, price: Int): Option[OfferAggregationActorState] = {

      if ( state.status == "closed" )  None

      else if ( state.offerAggregationStatus.count == 0 ){
        Some(state.copy(offerAggregationStatus = OfferAggregation(price,price,price,1)))
      }

      else {
        val min = if (state.offerAggregationStatus.min > price)  price else  state.offerAggregationStatus.min
        val max = if (state.offerAggregationStatus.max < price)  price else  state.offerAggregationStatus.max
        val avg = ((state.offerAggregationStatus.avg * state.offerAggregationStatus.count) + price) / (state.offerAggregationStatus.count + 1)
        val cnt = state.offerAggregationStatus.count + 1

        if (cnt == limit)
          Some(state.copy(status = "closed", offerAggregationStatus = state.offerAggregationStatus.copy(min, max, avg, cnt)))
        else Some(state.copy(offerAggregationStatus = state.offerAggregationStatus.copy(min, max, avg, cnt)))
      }
    }

    def closeAgg(state: OfferAggregationActorState ): Option[OfferAggregationActorState]= {
      if ( state.status.equals("closed")) None
      else {
        Some(state.copy(status = "closed"))
      }
    }

    def apply(): Behavior[Message] = Behaviors.setup { ctx =>

      var initialState: OfferAggregationActorState = OfferAggregationActorState("open", OfferAggregation(0,0,0,0))

          Behaviors.receiveMessage{

            case  Offer(replyTo, price, productCode) =>

            addAggregation(initialState, price).map(state => initialState = state) match {
              case Some(_) =>
                ctx.log.info(s"Offer added: $productCode, $price")
                replyTo ! SuccessMessage("aggregate added")
              case None =>
                ctx.log.error(s"Offer unable to add: $productCode, $price")
                replyTo ! ErrorMessage("unable add aggregate")
            }
            Behaviors.same
            case GetAggregateOffersByProduct(replyTo ,_) =>
            replyTo ! OfferAggregationReply(OfferAggregation(initialState.offerAggregationStatus.min,
              initialState.offerAggregationStatus.max,
              initialState.offerAggregationStatus.avg,
              initialState.offerAggregationStatus.count))
            Behaviors.same
            case Close(repl, _) =>
              closeAgg(initialState).map(state => initialState = state) match {
                case Some(_) => repl ! SuccessMessage("aggregation is closed")
                case None => repl ! ErrorMessage("aggregation is already closed")
              }
              Behaviors.same

          }
    }

  }

  object Guardian {
    implicit val timeout: Timeout = 3.seconds

  def apply(): Behavior[Message] = Behaviors.receive { (ctx,message) =>

    implicit val scheduler: typed.Scheduler = ctx.system.scheduler
    message match {
      case offer: Offer =>
        actorRefs.get(offer.productCode) match {
          case Some(actor) =>
            actor.ask(replyTo => Offer(replyTo,offer.price,offer.productCode)).onComplete {
        case Failure(exception) => exception
        case Success(done) => offer.replyTo ! done
        }

          case None =>
            val actorRef = ctx.spawn(OfferAggregationActor(), offer.productCode)
            actorRefs.put(offer.productCode, actorRef)
            actorRef.ask(replyTo => Offer(replyTo,offer.price,offer.productCode)).onComplete {
              case Failure(exception) => exception
              case Success(done) => offer.replyTo ! done
            }
        }
        Behaviors.same
      case getagg: GetAggregateOffersByProduct =>
        actorRefs.get(getagg.productCode) match {
          case Some(actor) =>
            actor.ask(reply => GetAggregateOffersByProduct(reply,getagg.productCode)).onComplete {
              case Failure(exception) => exception
              case Success(response) => getagg.replyTo ! response
            }
          case None => getagg.replyTo ! OfferAggregationReply(OfferAggregation(0,0,0,0))
        }
        Behaviors.same
      case Close(repl, productCode) =>
        actorRefs.get(productCode) match {
          case Some(actor) =>
            actor.ask(reply => Close(reply,productCode )).onComplete {
              case Failure(exception) => exception
              case Success(response) => repl ! response
            }
          case None => repl ! ErrorMessage("product does not exist")
        }
        Behaviors.same
    }
  }
}

    implicit val offerApi = jsonFormat2(OfferApi)
    implicit val offerAggregation = jsonFormat4(OfferAggregation)
    implicit val offerAggregationReply: RootJsonFormat[OfferAggregationReply] = jsonFormat1(OfferAggregationReply)


    object Run {

      implicit val system: ActorSystem[Message] = ActorSystem(Guardian(), "aggregate")


      val aggregationActor: ActorRef[Message] = system
      implicit val timeout: Timeout = 3.seconds

      implicit val executionContext = system.executionContext
      val route =
        pathPrefix("aggregation") {
          concat(
            path("getaggregation" ){
              get {
                  parameters("productCode".as[String]){ productCode=>
                    implicit val timeout: Timeout = 5.seconds

                    val res = aggregationActor.ask(repl => GetAggregateOffersByProduct(repl, productCode))

                    onComplete(res) {
                      case Failure(exception) =>
                        system.log.error(s"unable run: ${exception.getMessage}")
                        complete(StatusCodes.BadRequest,"aggregation does not exists")
                      case Success(value) =>
                        if (value.offerAggregation.count == 0 )complete(StatusCodes.BadRequest,"aggregation does not exists")
                        else complete(StatusCodes.OK, value)
                    }
                  }
                  }

                }
            ,
              post {
              path("offer") {
                entity(as[OfferApi]) { offer =>
                  val res = aggregationActor.ask(repl => Offer(repl,offer.price,offer.productCode))

                  onComplete(res) {
                    case Failure(exception) =>
                      system.log.error(exception.getMessage)
                      complete(StatusCodes.BadRequest,"unable complete request")
                    case Success(message) => message match {
                      case ErrorMessage(error) => complete(StatusCodes.BadRequest, error)
                      case SuccessMessage(msg) => complete(StatusCodes.OK, msg)
                    }
                  }
                }
              }
            }
            ,
            post {
              path("batchOffer") {
                entity(as[List[OfferApi]]) { offerApiList =>
                   val stream = Source(offerApiList)
                     .buffer(100, OverflowStrategy.backpressure)
                    .map{
                      element =>
                        aggregationActor.ask(repl => Offer(repl,element.price,element.productCode))
                    }
                    .runWith(Sink.seq)
                    .map(f => Future.sequence(f))
                    .flatten




                  onComplete(stream) {
                       case Failure(exception) =>
                         complete(StatusCodes.BadRequest, exception.getMessage)
                       case Success(value) =>
                       complete(StatusCodes.OK, value.map(a => a.toString))
                     }
                }
              }
            }
            ,
            put {
              path("close") {
                parameters("productCode".as[String]) { productCode =>
                  val res = aggregationActor.ask(repl => Close(repl,productCode))
                  onComplete(res) {
                    case Failure(exception) =>
                      system.log.error(exception.getMessage)
                      complete(StatusCodes.BadRequest,"unable complete request")
                    case Success(message) => message match {
                      case ErrorMessage(error) => complete(StatusCodes.BadRequest, error)
                      case SuccessMessage(msg) => complete(StatusCodes.OK, msg)
                    }
                  }
                }
              }
            }
          )
        }

      def apply() = {
        val bindingFuture = Http().newServerAt("localhost", 8088).bind(route)
        println(s"Server online at http://localhost:8088/\nPress RETURN to stop...")
        StdIn.readLine()
        bindingFuture
          .flatMap(_.unbind())(executionContext)
          .onComplete(_ => system.terminate())(executionContext)
      }
    }

  def main(args: Array[String]): Unit = {
    Run()
  }

}