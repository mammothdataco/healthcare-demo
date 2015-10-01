package com.mammothdata.nlp

import java.io.File
import java.util.regex.Pattern
import scala.Array.canBuildFrom

case class Offset(val start: Int, val end: Int) {
  def isNone = start == -1 && end == -1
  def None = Offset(-1,-1)
}

class Negex(ruleList: Array[String], responses: List[String]) extends java.io.Serializable {

  val rules = sortRules(ruleList)
  val MAX_WORD_DISTANCE = 5
  
  def predict(sentence: String, phrase: String): String = 
    if (negTagger(sentence, phrase)) 
      responses(0)
    else responses(1)

  private def sortRules(ruleFile: Array[String]): List[(Pattern,String)] = {
    ruleFile
      // input format: trigger phrase\t\t[TYPE]
      .map(line => {
        val cols = line.split("\t\t")
        (cols(0), cols(1))
      })
      .toList
      // sort by length descending
      .sortWith((a,b) => a._1.length > b._1.length)
      // replace spaces by \\s+ and convert to pattern
      .map(pair => (
        Pattern.compile("\\b(" + pair._1
          .trim()
          .replaceAll("\\s+", "\\\\s+") + ")\\b"), 
            pair._2))
  }

  def phrasePattern(phrase: String): Pattern = {
    Pattern.compile(
      "\\b(" + 
      phrase.replaceAll("\\s+", "\\\\s+") + 
      ")\\b", Pattern.CASE_INSENSITIVE)
  }
  

  def wordPositions(s: String): List[Int] = {
    0 :: s.toCharArray()
      .zipWithIndex
      .filter(ci => ci._1 == ' ')
      .map(ci => ci._2 + 1)
      .toList
  }

  def negTagger(sentence: String, phrase: String): Boolean = {
    val normSent = sentence.toLowerCase().replaceAll("\\s+", " ")
    tagging(normSent, phrase)
  }

  def tagging(normSent: String, phrase: String): Boolean = {
    val phraseOffset = offset(normSent, phrasePattern(phrase))
    if (phraseOffset.isNone) return false
    // look for CONJ trigger terms
    val conjOffsets = offsets(normSent, "[CONJ]", rules)
    conjOffsets.isEmpty match {
      case true => isTriggerInScope(normSent, rules, phraseOffset, wordPositions(normSent), List("[PREN]", "[POST]")) 
      case false =>   { 
        val conjOffset = conjOffsets.head
        conjOffset match {
          case _ if(conjOffset.end < phraseOffset.start) => tagging(normSent.substring(conjOffset.end + 1), phrase)
          case _ if(phraseOffset.end < conjOffset.start) => tagging(normSent.substring(0, conjOffset.start), phrase)
          case _ => false
        }
      }
    }
  }
  
  def isTriggerInScope(sentence: String,rules: List[(Pattern,String)],
                       phraseOffset: Offset,wordPositions: List[Int],
                       triggerTypes: List[String]): Boolean = {
    if (triggerTypes.isEmpty) false
    else {
      val currentTriggerType = triggerTypes.head
      val triggerOffsets = offsets(sentence, currentTriggerType, rules)
      val selectedTriggerOffset = firstNonOverlappingOffset(phraseOffset, triggerOffsets)
      selectedTriggerOffset.isNone match {
        case true =>  isTriggerInScope(sentence, rules, phraseOffset, wordPositions, triggerTypes.tail)
        case false =>  withinWordDistance(selectedTriggerOffset, phraseOffset, wordPositions)
      }
    }
  }
  
  def withinWordDistance(trigger: Offset, phrase: Offset, wordPositions: List[Int]): Boolean = {
    (trigger.start < phrase.start) && 
    wordDistance(phrase, trigger, wordPositions) < MAX_WORD_DISTANCE
  }

  def wordDistance(phraseOffset: Offset, triggerOffset: Offset, wordPositions: List[Int]): Int = {
    def check(i: Int, first: Offset, second: Offset): Boolean = {
      i > first.end && i < second.start
    }
    phraseOffset match {
      case _ if (phraseOffset.start < triggerOffset.start) =>  wordPositions.filter(check(_, phraseOffset, triggerOffset)).size
      case _ => wordPositions.filter(check(_, triggerOffset, phraseOffset)).size
    }   
  }
  
  def offset(sentence: String, 
      pattern: Pattern): Offset = {
    val matcher = pattern.matcher(sentence)
    if (matcher.find()) 
      Offset(matcher.start(), matcher.end())
    else Offset(-1, -1)      
  }

  def offsets(sentence: String, ruleType: String, rules: List[(Pattern,String)]): List[Offset] = {
    rules.filter(rule => ruleType.equals(rule._2))
         .map(rule => offset(sentence, rule._1))
         .filter(offset => (!offset.isNone))
  }
  
  def firstNonOverlappingOffset(phraseOffset: Offset, triggerOffsets: List[Offset]): Offset = {  
    val phraseRange = Range(phraseOffset.start, phraseOffset.end)
    val nonOverlaps = triggerOffsets
      .filter(offset => {
        val offsetRange = Range(offset.start, offset.end)  
        phraseRange.intersect(offsetRange).size == 0
      })
    if (nonOverlaps.isEmpty) Offset(-1,-1) 
    else nonOverlaps.head
  }
}
