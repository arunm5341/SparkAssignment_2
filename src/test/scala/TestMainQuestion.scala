import MainQuestion.LogLine
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

class TestMainQuestion extends AnyFunSuite with BeforeAndAfterEach{
assert(MainQuestion.LogLine(debug_level =, timestamp = , download_id = , retrieval_stage = , rest = )===
  LogLine(debug_level = ,timestamp = ???, download_id = ???, retrieval_stage = ???, rest = ???)
)
}
