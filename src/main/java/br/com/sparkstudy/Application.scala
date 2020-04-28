package br.com.sparkstudy

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object Application {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("SparkStudy").master("local").getOrCreate()

    import sparkSession.implicits._

    val precos1 = sparkSession.read.json("/home/jhona/dev/workspace-spark/SparkStudy/src/main/resources/listaJson1.json")
      .withColumn("udfResult", explode(mapearEstadosProduto($"estadoProduto")))
      .withColumn("estadoProduto", $"udfResult._1")
      .withColumn("codigoComercializacao_1", $"udfResult._2")
      .withColumn("precoCartaz_1", $"udfResult._3")
      .withColumn("precoMinimo_1", $"udfResult._4")
      .withColumn("precoLimite_1", $"udfResult._5")
      .drop($"udfResult")


    val precos2 = sparkSession.read.json("/home/jhona/dev/workspace-spark/SparkStudy/src/main/resources/listaJson2.json")
      .withColumn("udfResult", explode(mapearEstadosProduto($"estadoProduto")))
      .withColumn("estadoProduto", $"udfResult._1")
      .withColumn("codigoComercializacao_2", $"udfResult._2")
      .withColumn("precoCartaz_2", $"udfResult._3")
      .withColumn("precoMinimo_2", $"udfResult._4")
      .withColumn("precoLimite_2", $"udfResult._5")
      .drop($"udfResult")

    val precosJoin = precos1.join(precos2, Seq("_id", "estadoProduto"), "left")
      .na.fill(0, Seq("codigoComercializacao_1", "codigoComercializacao_2"))
      .na.fill("0", Seq("precoCartaz_1", "precoMinimo_1", "precoLimite_1", "precoCartaz_2", "precoMinimo_2", "precoLimite_2"))

    precosJoin.show(false)

    val diferencas = precosJoin.filter("codigoComercializacao_1 <> codigoComercializacao_2 OR precoCartaz_1 <> precoCartaz_2 OR precoMinimo_1 <> precoMinimo_2 OR precoLimite_1 <> precoLimite_2")
    diferencas.show(false)

    println(diferencas.count())


    //TODO id que existe na antiga e não existe na nova
    //todo id que existe na nova e não existe na antiga
    //TODO comercialização que existe na antiga e não existe na nova
    //TODO comercialização que existe na nova e que não existe na antiga
    //TODO Comercialização que está diferente na nova
    //TODO valor preço cartaz que está diferente na nova
    //TODO valor preço minimo que está diferente na nova
    //TODO valor preço limite que está diferente na nova
    //TODO valor preço sugestão que está diferente na nova
    //TODO valor preço custo que está diferente na nova

    //TODO atualizar divergencia apenas na filial necessária
  }

  //  def mapStructs: UserDefinedFunction = udf((r: Row) => {
  //    r.schema.fields.map(f => (
  //      f.name,
  //      r.getAs[Row](f.name).getAs[Long]("comercializacao"),
  //      r.getAs[Row](f.name).getAs[Row]("preco").getAs[String]("por")
  //    ))
  //  })

  def mapearEstadosProduto: UserDefinedFunction = udf((r: Row) => {
    r.schema.fields.map(f => (
      f.name,
      r.getAs[Row](f.name).getAs[Long]("comercializacao"),
      obterPrecoCartaz(r.getAs[Row](f.name)),
      obterPrecoMinimo(r.getAs[Row](f.name)),
      obterPrecoLimite(r.getAs[Row](f.name))
    ))
  })

  def obterPrecoCartaz(r: Row): String = {
    if (!r.schema.fieldNames.contains("preco")) return null
    r.getAs[Row]("preco").getAs[String]("por")
  }

  def obterPrecoMinimo(r: Row): String = {
    if (!r.schema.fieldNames.contains("precoMinimo")) return null
    r.getAs[Row]("precoMinimo").getAs[String]("por")
  }

  def obterPrecoLimite(r: Row): String = {
    if (!r.schema.fieldNames.contains("precoLimite")) return null
    r.getAs[Row]("precoLimite").getAs[String]("por")
  }
}
