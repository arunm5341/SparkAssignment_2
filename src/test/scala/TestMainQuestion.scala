import MainQuestion.LogLine
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

class TestMainQuestion extends AnyFunSuite with BeforeAndAfterEach{
  //assert(MainQuestion.dateFormat(2021-12-23)==2021-12-23)

assert(MainQuestion.LogLine(debug_level ="abc", timestamp= ???, download_id =12 , retrieval_stage ="xyz" , rest ="mnc" )===
  ("abc" , ???,12,"xyz","mnc"))
}
