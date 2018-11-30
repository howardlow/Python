# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#All the imports required for this assignment
import json
import re
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import lit

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression, OneVsRest

from pyspark.sql.window import Window as W
from pyspark.sql import functions as F

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

from pyspark.sql.functions import monotonically_increasing_id

from pyspark.mllib.evaluation import RankingMetrics