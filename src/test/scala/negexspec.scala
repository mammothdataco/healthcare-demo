import org.scalatest._
import java.io.File
import com.mammothdata.nlp._
import scala.io.Source

class NegexSpec extends FlatSpec with Matchers {

  def fixture = new {
    val path = "negex.txt"
    val positive = "cause of infection"
    val negative = "No evidence of infection"
    val noMatch = "And then, a cloud!"
    val keyword = "infection"
    val wrongNegTest = "Chest radiograph:  Frontal view obtained **DATE[Mar 24 2008], which revealed likely   MILD BIBASILAR ATELECTASIS, less likely pneumonia (cannot be excluded)."
    val wrongNegPhrase = "mild bibasilar atelectasis"
    val negex = new Negex(Source.fromFile(path).getLines().toArray, List("Negated", "Affirmed"))
  }

  "The NegexAnnotator" should "detect a positive sentence" in {
    val annotation = fixture.negex.predict(fixture.positive, fixture.keyword) 
    assert(annotation == "Affirmed")
  }

  it should "detect a simple negation" in {
    val annotation = fixture.negex.predict(fixture.negative, fixture.keyword) 
    assert(annotation == "Negated")
  }

  "the offset" should "return a valid offset with a match" in {
    val offset = fixture.negex.offset(fixture.positive, fixture.negex.phrasePattern(fixture.keyword))
    offset should not be (Offset(-1,-1))
  } 

  "the offset" should "return a non-valid offset without a match" in {
    val offset = fixture.negex.offset(fixture.noMatch, fixture.negex.phrasePattern(fixture.keyword))
    offset should be (Offset(-1,-1))
  } 
}