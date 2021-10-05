hdfs dfs -copyFromLocal '/home/cloudera/git/SimplePageRank/file/PageRank.txt' InputFolder

val loadfile = sc.textFile("InputFolder/PageRank.txt")
System.out.println(loadfile.collect().mkString("\n"))

val pairs = loadfile.map{line =>
val parts = line.split("->");
(parts(0), parts(1));
}
System.out.println(pairs.collect().mkString("\n"))

val nodes = pairs.flatMap{ case(nodeId, outlinks) =>
val outlink = outlinks.split(",");
outlink.map(ot => (ot,0.85/outlink.size));
}
System.out.println(nodes.collect().mkString("\n"))

val nodeGroup = nodes.reduceByKey(_+_)
System.out.println(nodeGroup.collect().mkString("\n"))


val pr = nodeGroup.map{ case (node,rank) => (node,rank)}.sortBy(-_._2)
System.out.println(pr.collect().mkString("\n"))

val pagerankDF = pr.toDF("nodeId","pagerank")
pagerankDF.show()

val inlinks = sc.parallelize(nodes.countByKey().toSeq).map{ case (node,count) => (node,count)}
System.out.println(inlinks.collect().mkString("\n"))

val inlinksDF = inlinks.toDF("nodeId","inlinks")
inlinksDF.show()

val prInlinksDF = pagerankDF.join(inlinksDF,pagerankDF("nodeId") === inlinksDF("nodeId"), "inner").drop(inlinksDF("nodeId"))
prInlinksDF.show()

val outlinks = pairs.map{ case (nodeId, outNodes) =>
val outlinkNodes = outNodes.split(",");
(nodeId,outlinkNodes.size);
}
System.out.println(outlinks.collect().mkString("\n"))

val outlinksDF = outlinks.toDF("nodeId", "outlinks")
outlinksDF.show()

val prInOutLinks = outlinksDF.join(prInlinksDF, outlinksDF("nodeId") === prInlinksDF("nodeId"), "leftouter").drop(prInlinksDF("nodeId"))
prInOutLinks.show()

val prInOutLinksDF = prInOutLinks.na.fill(0)
val result = prInOutLinksDF.withColumn("pagerank", col("pagerank")+0.15).sort($"pagerank".desc)

result.select($"pagerank",$"inlinks",$"outlinks").show(500)