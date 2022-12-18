
import aggregateService.Service
import aggregateService.Service.{ OfferApi}
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.{ActorRef}
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ServiceSpec extends AnyWordSpec with Matchers with ScalaFutures with ScalatestRouteTest{

  lazy val testKit = ActorTestKit()
  implicit def typedSystem: ActorRef[Service.Message] = testKit.spawn(Service.Guardian())

  lazy val routes = Service.Run.route
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._



  "AggregationRoutes" should {
    "be able to add aggregation (POST /aggregation/offer)" in {
      val offerApi = OfferApi(100,"some1")
      val offerEntity = Marshal(offerApi).to[MessageEntity].futureValue

      val request = Post("/aggregation/offer").withEntity(offerEntity)

      request ~> routes ~> check {
        status should ===(StatusCodes.OK)

        contentType should ===(ContentTypes.`text/plain(UTF-8)`)

        entityAs[String] should ===("aggregate added")
      }
    }


    "return aggregation (GET /aggregation/getaggregation)" in {

      val offerApi = OfferApi(200,"some1")
      val offerEntity = Marshal(offerApi).to[MessageEntity].futureValue
      val postRequest = Post("/aggregation/offer").withEntity(offerEntity)

      postRequest ~> routes ~> check {
        status should ===(StatusCodes.OK)
        contentType should ===(ContentTypes.`text/plain(UTF-8)`)
        entityAs[String] should ===("aggregate added")
      }



      val request = Get(uri = "/aggregation/getaggregation?productCode=some1")

      request ~> routes ~> check {
                println(entityAs[String])
        status should ===(StatusCodes.OK)
        contentType should === (ContentTypes.`application/json`)
        entityAs[String] should ===("""{"offerAggregation":{"avg":150,"count":2,"max":200,"min":100}}""")
      }
    }

    "be able to close aggregation(/aggregation/close?productCode=some1)" in {

      val request = Put(uri = "/aggregation/close?productCode=some1")

      request ~> routes ~> check {
        status should ===(StatusCodes.OK)
        contentType should ===(ContentTypes.`text/plain(UTF-8)`)
        entityAs[String] should ===("aggregation is closed")
      }
    }
  }
}
