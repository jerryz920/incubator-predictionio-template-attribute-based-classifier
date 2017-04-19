package org.template.classification

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

case class DataSourceParams(
  appName: String,
  evalK: Option[Int]  // define the k-fold parameter.
) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    val labeledPoints: RDD[LabeledPoint] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user",
      // only keep entities with these required properties defined
      required = Some(List("year", 
                           "MSD_0", "MSD_1", "MSD_2", "MSD_3", "MSD_4", "MSD_5", "MSD_6", "MSD_7", "MSD_8", "MSD_9",
                           "MSD_10", "MSD_11", "MSD_12", "MSD_13", "MSD_14", "MSD_15", "MSD_16", "MSD_17", "MSD_18", "MSD_19",
                           "MSD_20", "MSD_21", "MSD_22", "MSD_23", "MSD_24", "MSD_25", "MSD_26", "MSD_27", "MSD_28", "MSD_29",
                           "MSD_30", "MSD_31", "MSD_32", "MSD_33", "MSD_34", "MSD_35", "MSD_36", "MSD_37", "MSD_38", "MSD_39",
                           "MSD_40", "MSD_41", "MSD_42", "MSD_43", "MSD_44", "MSD_45", "MSD_46", "MSD_47", "MSD_48", "MSD_49",
                           "MSD_50", "MSD_51", "MSD_52", "MSD_53", "MSD_54", "MSD_55", "MSD_56", "MSD_57", "MSD_58", "MSD_59",
                           "MSD_60", "MSD_61", "MSD_62", "MSD_63", "MSD_64", "MSD_65", "MSD_66", "MSD_67", "MSD_68", "MSD_69",
                           "MSD_70", "MSD_71", "MSD_72", "MSD_73", "MSD_74", "MSD_75", "MSD_76", "MSD_77", "MSD_78", "MSD_79",
                           "MSD_80", "MSD_81", "MSD_82", "MSD_83", "MSD_84", "MSD_85", "MSD_86", "MSD_87", "MSD_88"
                           )))(sc)
      // aggregateProperties() returns RDD pair of
      // entity ID and its aggregated properties
      .map { case (entityId, properties) =>
        try {
          LabeledPoint(properties.get[Double]("year"),
            Vectors.dense(Array(
              properties.get[Double]("MSD_0"),
              properties.get[Double]("MSD_1"),
              properties.get[Double]("MSD_2"),
              properties.get[Double]("MSD_3"),
              properties.get[Double]("MSD_4"),
              properties.get[Double]("MSD_5"),
              properties.get[Double]("MSD_6"),
              properties.get[Double]("MSD_7"),
              properties.get[Double]("MSD_8"),
              properties.get[Double]("MSD_9"),
              properties.get[Double]("MSD_10"),
              properties.get[Double]("MSD_11"),
              properties.get[Double]("MSD_12"),
              properties.get[Double]("MSD_13"),
              properties.get[Double]("MSD_14"),
              properties.get[Double]("MSD_15"),
              properties.get[Double]("MSD_16"),
              properties.get[Double]("MSD_17"),
              properties.get[Double]("MSD_18"),
              properties.get[Double]("MSD_19"),
              properties.get[Double]("MSD_20"),
              properties.get[Double]("MSD_21"),
              properties.get[Double]("MSD_22"),
              properties.get[Double]("MSD_23"),
              properties.get[Double]("MSD_24"),
              properties.get[Double]("MSD_25"),
              properties.get[Double]("MSD_26"),
              properties.get[Double]("MSD_27"),
              properties.get[Double]("MSD_28"),
              properties.get[Double]("MSD_29"),
              properties.get[Double]("MSD_30"),
              properties.get[Double]("MSD_31"),
              properties.get[Double]("MSD_32"),
              properties.get[Double]("MSD_33"),
              properties.get[Double]("MSD_34"),
              properties.get[Double]("MSD_35"),
              properties.get[Double]("MSD_36"),
              properties.get[Double]("MSD_37"),
              properties.get[Double]("MSD_38"),
              properties.get[Double]("MSD_39"),
              properties.get[Double]("MSD_40"),
              properties.get[Double]("MSD_41"),
              properties.get[Double]("MSD_42"),
              properties.get[Double]("MSD_43"),
              properties.get[Double]("MSD_44"),
              properties.get[Double]("MSD_45"),
              properties.get[Double]("MSD_46"),
              properties.get[Double]("MSD_47"),
              properties.get[Double]("MSD_48"),
              properties.get[Double]("MSD_49"),
              properties.get[Double]("MSD_50"),
              properties.get[Double]("MSD_51"),
              properties.get[Double]("MSD_52"),
              properties.get[Double]("MSD_53"),
              properties.get[Double]("MSD_54"),
              properties.get[Double]("MSD_55"),
              properties.get[Double]("MSD_56"),
              properties.get[Double]("MSD_57"),
              properties.get[Double]("MSD_58"),
              properties.get[Double]("MSD_59"),
              properties.get[Double]("MSD_60"),
              properties.get[Double]("MSD_61"),
              properties.get[Double]("MSD_62"),
              properties.get[Double]("MSD_63"),
              properties.get[Double]("MSD_64"),
              properties.get[Double]("MSD_65"),
              properties.get[Double]("MSD_66"),
              properties.get[Double]("MSD_67"),
              properties.get[Double]("MSD_68"),
              properties.get[Double]("MSD_69"),
              properties.get[Double]("MSD_70"),
              properties.get[Double]("MSD_71"),
              properties.get[Double]("MSD_72"),
              properties.get[Double]("MSD_73"),
              properties.get[Double]("MSD_74"),
              properties.get[Double]("MSD_75"),
              properties.get[Double]("MSD_77"),
              properties.get[Double]("MSD_78"),
              properties.get[Double]("MSD_79"),
              properties.get[Double]("MSD_80"),
              properties.get[Double]("MSD_81"),
              properties.get[Double]("MSD_82"),
              properties.get[Double]("MSD_83"),
              properties.get[Double]("MSD_84"),
              properties.get[Double]("MSD_85"),
              properties.get[Double]("MSD_86"),
              properties.get[Double]("MSD_87"),
              properties.get[Double]("MSD_88")
            ))
          )
        } catch {
          case e: Exception => {
            logger.error(s"Failed to get properties ${properties} of" +
              s" ${entityId}. Exception: ${e}.")
            throw e
          }
        }
      }.cache()

    new TrainingData(labeledPoints)
  }

  override
  def readEval(sc: SparkContext)
  : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
    require(dsp.evalK.nonEmpty, "DataSourceParams.evalK must not be None")

    // The following code reads the data from data store. It is equivalent to
    // the readTraining method. We copy-and-paste the exact code here for
    // illustration purpose, a recommended approach is to factor out this logic
    // into a helper function and have both readTraining and readEval call the
    // helper.
    val labeledPoints: RDD[LabeledPoint] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user",
      // only keep entities with these required properties defined
      required = Some(List("year", 
                           "MSD_0", "MSD_1", "MSD_2", "MSD_3", "MSD_4", "MSD_5", "MSD_6", "MSD_7", "MSD_8", "MSD_9",
                           "MSD_10", "MSD_11", "MSD_12", "MSD_13", "MSD_14", "MSD_15", "MSD_16", "MSD_17", "MSD_18", "MSD_19",
                           "MSD_20", "MSD_21", "MSD_22", "MSD_23", "MSD_24", "MSD_25", "MSD_26", "MSD_27", "MSD_28", "MSD_29",
                           "MSD_30", "MSD_31", "MSD_32", "MSD_33", "MSD_34", "MSD_35", "MSD_36", "MSD_37", "MSD_38", "MSD_39",
                           "MSD_40", "MSD_41", "MSD_42", "MSD_43", "MSD_44", "MSD_45", "MSD_46", "MSD_47", "MSD_48", "MSD_49",
                           "MSD_50", "MSD_51", "MSD_52", "MSD_53", "MSD_54", "MSD_55", "MSD_56", "MSD_57", "MSD_58", "MSD_59",
                           "MSD_60", "MSD_61", "MSD_62", "MSD_63", "MSD_64", "MSD_65", "MSD_66", "MSD_67", "MSD_68", "MSD_69",
                           "MSD_70", "MSD_71", "MSD_72", "MSD_73", "MSD_74", "MSD_75", "MSD_76", "MSD_77", "MSD_78", "MSD_79",
                           "MSD_80", "MSD_81", "MSD_82", "MSD_83", "MSD_84", "MSD_85", "MSD_86", "MSD_87", "MSD_88"
                           )))(sc)
      // aggregateProperties() returns RDD pair of
      // entity ID and its aggregated properties
      .map { case (entityId, properties) =>
        try {
          LabeledPoint(properties.get[Double]("year"),
            Vectors.dense(Array(
              properties.get[Double]("MSD_0"),
              properties.get[Double]("MSD_1"),
              properties.get[Double]("MSD_2"),
              properties.get[Double]("MSD_3"),
              properties.get[Double]("MSD_4"),
              properties.get[Double]("MSD_5"),
              properties.get[Double]("MSD_6"),
              properties.get[Double]("MSD_7"),
              properties.get[Double]("MSD_8"),
              properties.get[Double]("MSD_9"),
              properties.get[Double]("MSD_10"),
              properties.get[Double]("MSD_11"),
              properties.get[Double]("MSD_12"),
              properties.get[Double]("MSD_13"),
              properties.get[Double]("MSD_14"),
              properties.get[Double]("MSD_15"),
              properties.get[Double]("MSD_16"),
              properties.get[Double]("MSD_17"),
              properties.get[Double]("MSD_18"),
              properties.get[Double]("MSD_19"),
              properties.get[Double]("MSD_20"),
              properties.get[Double]("MSD_21"),
              properties.get[Double]("MSD_22"),
              properties.get[Double]("MSD_23"),
              properties.get[Double]("MSD_24"),
              properties.get[Double]("MSD_25"),
              properties.get[Double]("MSD_26"),
              properties.get[Double]("MSD_27"),
              properties.get[Double]("MSD_28"),
              properties.get[Double]("MSD_29"),
              properties.get[Double]("MSD_30"),
              properties.get[Double]("MSD_31"),
              properties.get[Double]("MSD_32"),
              properties.get[Double]("MSD_33"),
              properties.get[Double]("MSD_34"),
              properties.get[Double]("MSD_35"),
              properties.get[Double]("MSD_36"),
              properties.get[Double]("MSD_37"),
              properties.get[Double]("MSD_38"),
              properties.get[Double]("MSD_39"),
              properties.get[Double]("MSD_40"),
              properties.get[Double]("MSD_41"),
              properties.get[Double]("MSD_42"),
              properties.get[Double]("MSD_43"),
              properties.get[Double]("MSD_44"),
              properties.get[Double]("MSD_45"),
              properties.get[Double]("MSD_46"),
              properties.get[Double]("MSD_47"),
              properties.get[Double]("MSD_48"),
              properties.get[Double]("MSD_49"),
              properties.get[Double]("MSD_50"),
              properties.get[Double]("MSD_51"),
              properties.get[Double]("MSD_52"),
              properties.get[Double]("MSD_53"),
              properties.get[Double]("MSD_54"),
              properties.get[Double]("MSD_55"),
              properties.get[Double]("MSD_56"),
              properties.get[Double]("MSD_57"),
              properties.get[Double]("MSD_58"),
              properties.get[Double]("MSD_59"),
              properties.get[Double]("MSD_60"),
              properties.get[Double]("MSD_61"),
              properties.get[Double]("MSD_62"),
              properties.get[Double]("MSD_63"),
              properties.get[Double]("MSD_64"),
              properties.get[Double]("MSD_65"),
              properties.get[Double]("MSD_66"),
              properties.get[Double]("MSD_67"),
              properties.get[Double]("MSD_68"),
              properties.get[Double]("MSD_69"),
              properties.get[Double]("MSD_70"),
              properties.get[Double]("MSD_71"),
              properties.get[Double]("MSD_72"),
              properties.get[Double]("MSD_73"),
              properties.get[Double]("MSD_74"),
              properties.get[Double]("MSD_75"),
              properties.get[Double]("MSD_77"),
              properties.get[Double]("MSD_78"),
              properties.get[Double]("MSD_79"),
              properties.get[Double]("MSD_80"),
              properties.get[Double]("MSD_81"),
              properties.get[Double]("MSD_82"),
              properties.get[Double]("MSD_83"),
              properties.get[Double]("MSD_84"),
              properties.get[Double]("MSD_85"),
              properties.get[Double]("MSD_86"),
              properties.get[Double]("MSD_87"),
              properties.get[Double]("MSD_88")
            ))
          )
        } catch {
          case e: Exception => {
            logger.error(s"Failed to get properties ${properties} of" +
              s" ${entityId}. Exception: ${e}.")
            throw e
          }
        }
      }.cache()
    // End of reading from data store

    // K-fold splitting
    val evalK = dsp.evalK.get
    val indexedPoints: RDD[(LabeledPoint, Long)] = labeledPoints.zipWithIndex()

    (0 until evalK).map { idx =>
      val trainingPoints = indexedPoints.filter(_._2 % evalK != idx).map(_._1)
      val testingPoints = indexedPoints.filter(_._2 % evalK == idx).map(_._1)

      (
        new TrainingData(trainingPoints),
        new EmptyEvaluationInfo(),
        testingPoints.map {
          p => (new Query(p.features.toArray), new ActualResult(p.label)) 
        }
      )
    }
  }
}

class TrainingData(
  val labeledPoints: RDD[LabeledPoint]
) extends Serializable
