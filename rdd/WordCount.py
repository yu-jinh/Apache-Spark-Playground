from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[3]")
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")
    lines = sc.textFile("in/word_count.text")
    print(lines)
    words = lines.flatMap(lambda line: line.split(" "))
    print(words)
    wordCounts = words.countByValue()
    print()
    print(wordCounts)
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))


