# 用Python编写Hadoop下MapReduce程序

标签（空格分隔）： Hadoop MapReduce Python

---

本文是作者学习MapReduce编程的记录，几乎翻译自文章《[Writing an Hadoop MapReduce Program in Python][1]》。不当之处，还望海涵，多多指点。

Hadoop提供了Hadoop Streaming API，可以通过标准输入输出帮助我们在Map和Reduce代码之间传递数据。因此我们可以使用Python的sys.stdin来读取数据，并print到标准输出sys.stdout。其他部分，都有Hadoop Streaming来完成。

问题：
我们拥有的日志数据如下图所示，记录了用户上网的时间，用户id，ip地址(已屏蔽)，hostname等。最后两列是用户上行、下行流量。我们的任务是根据用户id统计每个用户总的上下行流量。
![数据说明](http://img-storage.qiniudn.com/15-6-13/97650736.jpg "数据说明")

Map step：mapper.py

```python
#! /usr/bin/python

import sys
import re

# input comes from STDIN (standard input)
for line in sys.stdin:
	 # remove leading and trailing whitespace
	 line = line.strip()
	 # split the line into words by white space,including black
	 # space,tab and so on
	 words = re.split('\s+',line)
	 # print uid,upload traffic,download traffic
	 print '%s\t%s\t%s' % (words[2],words[-2],words[-1])
```
map步骤输入为上图所示文件，输出为key：用户id + value：上行流量，下行流量

----
Reduce step：reducer.py
Reduce步骤只需要将相同key值的合并即可。

```python
#!/usr/bin/python

from operator import itemgetter
import sys

current_usrid = None
current_count1 = 0
current_count2 = 0
usrid = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip("\t")

    # parse the input we got from mapper.py
    usrid, count1, count2 = line.split('\t', 2)

    # convert count (currently a string) to int
    try:
        count1 = int(count1)
    except ValueError:
        print "%s is not a number" % count1 
        continue
    try:
        count2 = int(count2)
    except ValueError:
        print "%s is not a number" % count2
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_usrid == usrid:
        current_count1 += count1
        current_count2 += count2
    else:
        if current_usrid:
            # write result to STDOUT
            print '%s\t%s\t%s' % (current_usrid, current_count1, current_count2)
        current_count1 = count1
        current_count2 = count2
        current_usrid = usrid

# do not forget to output the last word if needed!
if current_usrid == usrid:
    print '%s\t%s\t%s' % (current_usrid, current_count1, current_count2)

```

测试代码：
用小数据在Linux本地检测:cat data | map | sort | reduce



  [1]: http://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
  
  
注意事项：
1. 文件权限
2. Linux文件和Windows文件格式的差别，可以使用dos2unix转换
3. Python空格、tab键的检查：python -m tabnanny yourfile.py
4. Hadoop的Streaming文件是：hadoop-streaming.jar（在0.20版本下）
5. find使用：find path -name filename
6. 在shell命令中使用Python程序时使用全路径或者相对路径

实验命令记录：

```shell
hadoop jar /opt/cloudera/parcels/CDH-5.3.0-1.cdh5.3.0.p0.30/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar \
-file /home/lvqinjie/file_zs/mapper.py    -mapper /home/lvqinjie/file_zs/mapper.py \
-file /home/lvqinjie/file_zs/reducer.py   -reducer /home/lvqinjie/file_zs/reducer.py \
-input /user/lvqinjie/file_zs/input/* -output /user/lvqinjie/file_zs/output/output1
```