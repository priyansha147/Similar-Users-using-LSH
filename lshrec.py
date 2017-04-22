from pyspark import SparkContext
from operator import add
import sys
import itertools

def get_topFive_list(IpLine):
  op = []
  IpLine.sort(key = lambda x: (-x[1], int(x[0][1:])))  
  val = IpLine[:5]
  for v in val:
        op.append(v[0])

  return sorted(op, key=lambda x:int(x[1:]))

def get_similar_users(IpLine):
    user1 = IpLine[0]
    user2 = IpLine[1][0]
    return ((user1, (user2, IpLine[1][1])), (user2, (user1, IpLine[1][1])))

def get_pairs(IpLine):
    op = []
    op = itertools.combinations(IpLine, 2)
    return op

def get_similarity(IpLine):
    IpLine = list(IpLine)
    user1 = set(dict_inp[IpLine[0]])
    user2 = set(dict_inp[IpLine[1]])
    inter = 0
    un = 0
    inter = len(user1 & user2)
    un = len(user1 | user2) 
    
    return (IpLine[0], (IpLine[1], float(inter)/un))

def get_rows(IpLine):
    op = []
    if IpLine[0] in indices:
      values = indices[IpLine[0]]
      for v in values:
        op.append((v, IpLine[1]))

    return op

def get_bands(IpLine):
    value = IpLine[0]
    k = IpLine[1]
    x = 0
    final = []
    while x <= 16:
       band = str(x) +','+ str(k[x]) +','+ str(k[x+1]) +','+ str(k[x+2]) +','+ str(k[x+3])
       final.append((band, value))
       x += 4
    return final
	
def get_signMatrix(IpLine):
    sign = [2000]*20
    for x in range(0, 100):
      if IpLine[x] == 1:
        for i in range(0, 20):
          h = (3*x + 13*i)%100
          if sign[i] > h:
            sign[i] = h
    return sign

def get_dictionary_input(IpLine):
    b = IpLine.split(",")
    return (b[0], b[1:])

def get_charMatrix(IpLine):
    l = IpLine.split(",")
    key = l[0]
    rem = l[1:]
    val = []
    for x in range(0, 100):
	     val.append(0)
    for v in rem:
	     val[int(v)] = 1
    return (key,val) 
   

sc = SparkContext(appName="LSH")
inp = sc.textFile(sys.argv[1])
char_matrix = inp.map(get_charMatrix)
dict_inp = inp.map(get_dictionary_input).collect()
dict_inp = dict(dict_inp)

sign_matrix = char_matrix.mapValues(get_signMatrix)
banding = sign_matrix.flatMap(get_bands)
pair_list = banding.groupByKey().mapValues(lambda x : list(x)).filter(lambda x : len(x[1])>1).map(lambda x : x[1]).flatMap(get_pairs).map(lambda x: tuple(map(str, x))).groupByKey().flatMap(lambda x: [(x[0], v) for v in set(x[1])])
#print pair_list
#exit()
jaccard_similarity = pair_list.map(get_similarity).flatMap(get_similar_users).groupByKey().mapValues(lambda x : list(x))
final_list = jaccard_similarity.mapValues(get_topFive_list).collect()

finalOP = sorted(final_list, key = lambda x : int(x[0][1:]))

fo = open(sys.argv[2], "w")
for value in finalOP:
  fo.write(value[0] + ":" + ','.join(value[1]) + "\r\n")
fo.close()
