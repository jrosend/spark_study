package br.com.sparkstudy

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object Application {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("SparkStudy").master("local").getOrCreate()
    import sparkSession.implicits._

    sparkSession.conf.set("spark.sql.shuffle.partitions", 6)
    sparkSession.conf.set("spark.executor.memory", "1g")

    val precosAntigos = sparkSession.read.json("/home/jhona/dev/precificacao_filial_1000_old.json")
      .filter("estadoProduto is not null")
      .drop("precoOnline", "_class", "precoCusto", "precoSugestao", "ultimaAtualizacao")
      .withColumn("udfResult", explode(mapearEstadosProduto($"estadoProduto")))
      .withColumn("estadoProduto", $"udfResult._1")
      .withColumn("codigoComercializacao_1", $"udfResult._2")
      .withColumn("precoCartaz_1", $"udfResult._3")
      .withColumn("precoMinimo_1", $"udfResult._4")
      .withColumn("precoLimite_1", $"udfResult._5")
      .drop($"udfResult")

    val precosNovos = sparkSession.read.json("/home/jhona/dev/precificacao_filial_1000_nova.json")
      .filter("estadoProduto is not null")
      .drop("precoOnline", "_class", "precoCusto", "precoSugestao", "ultimaAtualizacao")
      .withColumn("udfResult", explode(mapearEstadosProduto($"estadoProduto")))
      .withColumn("estadoProduto", $"udfResult._1")
      .withColumn("codigoComercializacao_2", $"udfResult._2")
      .withColumn("precoCartaz_2", $"udfResult._3")
      .withColumn("precoMinimo_2", $"udfResult._4")
      .withColumn("precoLimite_2", $"udfResult._5")
      .drop($"udfResult")

    val precosJoin = precosAntigos.join(precosNovos, Seq("_id", "estadoProduto"), "left")
      .na.fill(0, Seq("codigoComercializacao_1", "codigoComercializacao_2"))
      .na.fill("0", Seq("precoCartaz_1", "precoMinimo_1", "precoLimite_1", "precoCartaz_2", "precoMinimo_2", "precoLimite_2"))

    println()
    println(s"Total de comercializações: ${precosJoin.count()}")
    println(s"Total de precificações: ${precosJoin.select("_id").distinct().count()}")
    println()
    //IGUALDADES
    println("Igualdades:")
    val igualdades = precosJoin.filter("codigoComercializacao_1 = codigoComercializacao_2 AND precoCartaz_1 = precoCartaz_2 AND precoMinimo_1 = precoMinimo_2 AND precoLimite_1 = precoLimite_2")
    println("Amostra de igualdades:")
    igualdades.show(false)
    println($"Comercializações iguais: ${igualdades.count()}")
    println($"Precificações iguais: ${igualdades.select("_id").distinct().count()}")

    println()
    println("Diferenças:")
    val diferencas = precosJoin.filter("codigoComercializacao_1 <> codigoComercializacao_2 OR precoCartaz_1 <> precoCartaz_2 OR precoMinimo_1 <> precoMinimo_2 OR precoLimite_1 <> precoLimite_2")

    println("Amostra de preços diferentes:")
    diferencas.show(false)
    println(s"Quantidade de fiferencas: ${diferencas.count()}")

    println(s"Quantidade de diferencas PADRAO: ${diferencas.filter("estadoProduto = 'PADRAO'").count()}")
    println(s"Quantidade de diferencas SALDO: ${diferencas.filter("estadoProduto = 'SALDO'").count()}")
    println(s"Quantidade de diferencas MOSTRUARIO: ${diferencas.filter("estadoProduto = 'MOSTRUARIO'").count()}")

    println(s"Diferenças únicas: ${diferencas.select("_id").distinct().count()}")


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

  def mapearEstadosProduto: UserDefinedFunction = udf((r: Row) => {
    r.schema.fields.map(f => (
      f.name,
      obterCodigoComercializacao(r.getAs[Row](f.name)),
      obterPrecoCartaz(r.getAs[Row](f.name)),
      obterPrecoMinimo(r.getAs[Row](f.name)),
      obterPrecoLimite(r.getAs[Row](f.name))
    ))
  })

  def obterCodigoComercializacao(r: Row): Long = {
    val comercializacao = obterComercializacao(r)
    if (comercializacao == null) return 0
    comercializacao.getAs[Long]("codigo")
  }

  def obterComercializacao(r: Row): Row = {
    if (r == null) return null
    r.getAs[Row]("comercializacao")
  }

  def obterPrecoCartaz(r: Row): String = {
    val comercializacao = obterComercializacao(r)
    if (comercializacao == null) return null
    if (!comercializacao.schema.fieldNames.contains("preco")) return null
    if (comercializacao.getAs[Row]("preco") == null) return null
    comercializacao.getAs[Row]("preco").getAs[String]("por")
  }

  def obterPrecoMinimo(r: Row): String = {
    val comercializacao = obterComercializacao(r)
    if (comercializacao == null) return null
    if (!comercializacao.schema.fieldNames.contains("precoMinimo")) return null
    if (comercializacao.getAs[Row]("precoMinimo") == null) return null
    if (comercializacao.getAs[Row]("precoMinimo").schema.contains("por")) return null
    comercializacao.getAs[Row]("precoMinimo").getAs[String]("por")
  }

  def obterPrecoLimite(r: Row): String = {
    val comercializacao = obterComercializacao(r)
    if (comercializacao == null) return null
    if (!comercializacao.schema.fieldNames.contains("precoLimite")) return null
    if (comercializacao.getAs[Row]("precoLimite") == null) return null
    comercializacao.getAs[Row]("precoLimite").getAs[String]("por")
  }
}