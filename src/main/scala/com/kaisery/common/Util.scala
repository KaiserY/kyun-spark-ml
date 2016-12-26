package com.kaisery.common

import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable

object Util {

    def string2Vector(text: String, maxLength: Int): Vector = {

        val limit = if (text.length > maxLength) maxLength else text.length

        val maxCount = maxLength + 1

        val unicodePostionFeatureCount = 256 * 2 * maxLength

        val unicodeNumberFeatureCount = Features.maxId * maxCount

        var featureIndex, alphabet, number, dot, minus, at, colon = 0

        var featuresMap = new mutable.HashMap[Int, Double]

        val featureText = text.substring(0, limit)

        featureText.getBytes("Unicode").drop(2).foreach(byte => {
            val value = byte & 0xFF

            val index = featureIndex

            featureIndex += 256


            featuresMap += index + value -> 5.0
        })

        featureText.foreach({
            case char if '0' until '9' contains char => number += 1
            case char if 'a' until 'z' contains char => alphabet += 1
            case char if 'A' until 'Z' contains char => alphabet += 1
            case '.' => dot += 1
            case '-' => minus += 1
            case '@' => at += 1
            case ':' => colon += 1
            case _ =>
        })

        featureIndex = unicodePostionFeatureCount

        featuresMap += featureIndex + Features.NUMBER.id * maxCount + number -> 10.0
        featuresMap += featureIndex + Features.ALPHABET.id * maxCount + alphabet -> 10.0
        featuresMap += featureIndex + Features.DOT.id * maxCount + dot -> 20.0
        featuresMap += featureIndex + Features.MINUS.id * maxCount + minus -> 20.0
        featuresMap += featureIndex + Features.AT.id * maxCount + at -> 20.0
        featuresMap += featureIndex + Features.COLON.id * maxCount + colon -> 20.0

        Vectors.sparse(unicodePostionFeatureCount + unicodeNumberFeatureCount, featuresMap.toSeq)
    }
}
