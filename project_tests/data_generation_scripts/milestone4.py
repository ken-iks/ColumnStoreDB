#!/usr/bin/python
import sys, string
from random import choice
import random
from string import ascii_lowercase
from scipy.stats import beta, uniform
import numpy as np
import struct
import pandas as pd
import math

import data_gen_utils

# note this is the base path where we store the data files we generate
TEST_BASE_DIR = "/cs165/generated_data"

# note this is the base path that _POINTS_ to the data files we generate
DOCKER_TEST_BASE_DIR = "/cs165/staff_test"

#
# Example usage: 
#   python milestone4.py 10000 10000 10000 10000 42 1.0 50 ~/repo/cs165-docker-test-runner/test_data /cs165/staff_test
#


############################################################################
# Notes: You can generate your own scripts for generating data fairly easily by modifying this script.
# 
############################################################################

class ZipfianDistribution:
    def __init__(self,zipfianParam, numElements):
        self.zipfianParam = zipfianParam
        self.numElements = numElements
        self.H_s = ZipfianDistribution.computeHarmonic(zipfianParam, numElements)

    def computeHarmonic(zipfianParam, numElements):
        total = 0.0
        for k in range(1,numElements+1,1):
            total += (1.0/math.pow(k, zipfianParam))
        return total

    def drawRandomSample(self, unifSample):
        total = 0.0
        k = 0
        while (unifSample >= total):
            k += 1
            total += ((1.0/math.pow(k, self.zipfianParam)) / self.H_s)
        return k
    def createRandomNumpyArray(self,arraySize):
        array = np.random.uniform(size=(arraySize))
        vectorizedSampleFunc = np.vectorize(self.drawRandomSample)
        return vectorizedSampleFunc(array)


def generateDataMilestone4(dataSizeFact, dataSizeDim1, dataSizeDim2, dataSizeSelect, zipfianParam, numDistinctElements):
    outputFile1 = TEST_BASE_DIR + '/' + 'data5_fact.csv'
    outputFile2 = TEST_BASE_DIR + '/' + 'data5_dimension1.csv'
    outputFile3 = TEST_BASE_DIR + '/' + 'data5_dimension2.csv'
    outputFile4 = TEST_BASE_DIR + '/' + 'data5_selectivity1.csv'
    outputFile5 = TEST_BASE_DIR + '/' + 'data5_selectivity2.csv'

    header_line_fact = data_gen_utils.generateHeaderLine('db1', 'tbl5_fact', 4)
    header_line_dim1 = data_gen_utils.generateHeaderLine('db1', 'tbl5_dim1', 3)
    header_line_dim2 = data_gen_utils.generateHeaderLine('db1', 'tbl5_dim2', 2)
    header_line_sel1 = data_gen_utils.generateHeaderLine('db1', 'tbl5_sel1', 2)
    header_line_sel2 = data_gen_utils.generateHeaderLine('db1', 'tbl5_sel2', 2)
    outputFactTable = pd.DataFrame(np.random.randint(0, dataSizeFact/5, size=(dataSizeFact, 4)), columns =['col1', 'col2', 'col3', 'col4'])
    zipfDist = ZipfianDistribution(zipfianParam, numDistinctElements)
    # See Zipf's distribution (wikipedia) for a description of this distribution. 
    outputFactTable['col1'] = zipfDist.createRandomNumpyArray(dataSizeFact)
    outputFactTable['col3'] = np.full((dataSizeFact),1)
    outputFactTable['col4'] = np.random.randint(1, dataSizeDim2, size=(dataSizeFact))

    outputDimTable1 = pd.DataFrame(np.random.randint(0, dataSizeDim1/5, size=(dataSizeDim1, 3)), columns =['col1', 'col2', 'col3'])
    # joinable on col1 with fact table
    outputDimTable1['col1'] = zipfDist.createRandomNumpyArray(dataSizeDim1)
    # joinable on col2 with dimension table 2
    outputDimTable1['col2'] = np.random.randint(1, dataSizeDim2, size=(dataSizeDim1))

    outputDimTable2 = pd.DataFrame(np.random.randint(0, dataSizeDim2/5, size=(dataSizeDim2, 2)), columns =['col1', 'col2'])
    outputDimTable2['col1'] = np.arange(1,dataSizeDim2+1, 1)

    # join on different selectivities
    outputSelectTable1 = pd.DataFrame(np.random.randint(0, dataSizeSelect/5, size=(dataSizeSelect, 2)), columns=['col1', 'col2']) 
    outputSelectTable2 = pd.DataFrame(np.random.randint(0, dataSizeSelect/5, size=(dataSizeSelect, 2)), columns=['col1', 'col2']) 
    
    outputFactTable.to_csv(outputFile1, sep=',', index=False, header=header_line_fact, lineterminator='\n')
    outputDimTable1.to_csv(outputFile2, sep=',', index=False, header=header_line_dim1, lineterminator='\n')
    outputDimTable2.to_csv(outputFile3, sep=',', index=False, header=header_line_dim2, lineterminator='\n')
    outputSelectTable1.to_csv(outputFile4, sep=',', index=False, header=header_line_sel1, lineterminator='\n')
    outputSelectTable2.to_csv(outputFile5, sep=',', index=False, header=header_line_sel2, lineterminator='\n')
    return outputFactTable, outputDimTable1, outputDimTable2, outputSelectTable1, outputSelectTable2

def createTest45():
    # prelude
    output_file, exp_output_file = data_gen_utils.openFileHandles(45, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Creates tables for join tests\n')
    output_file.write('-- without any indexes\n')
    output_file.write('create(tbl,"tbl5_fact",db1,4)\n')
    output_file.write('create(col,"col1",db1.tbl5_fact)\n')
    output_file.write('create(col,"col2",db1.tbl5_fact)\n')
    output_file.write('create(col,"col3",db1.tbl5_fact)\n')
    output_file.write('create(col,"col4",db1.tbl5_fact)\n')
    output_file.write('load("'+DOCKER_TEST_BASE_DIR+'/data5_fact.csv")\n')
    output_file.write('--\n')
    output_file.write('create(tbl,"tbl5_dim1",db1,3)\n')
    output_file.write('create(col,"col1",db1.tbl5_dim1)\n')
    output_file.write('create(col,"col2",db1.tbl5_dim1)\n')
    output_file.write('create(col,"col3",db1.tbl5_dim1)\n')
    output_file.write('load("'+DOCKER_TEST_BASE_DIR+'/data5_dimension1.csv")\n')
    output_file.write('--\n')
    output_file.write('create(tbl,"tbl5_dim2",db1,2)\n')
    output_file.write('create(col,"col1",db1.tbl5_dim2)\n')
    output_file.write('create(col,"col2",db1.tbl5_dim2)\n')
    output_file.write('load("'+DOCKER_TEST_BASE_DIR+'/data5_dimension2.csv")\n')
    output_file.write('--\n')
    output_file.write('create(tbl,"tbl5_sel1",db1,2)\n')
    output_file.write('create(col,"col1",db1.tbl5_sel1)\n')
    output_file.write('create(col,"col2",db1.tbl5_sel1)\n')
    output_file.write('load("'+DOCKER_TEST_BASE_DIR+'/data5_selectivity1.csv")\n')
    output_file.write('--\n')
    output_file.write('create(tbl,"tbl5_sel2",db1,2)\n')
    output_file.write('create(col,"col1",db1.tbl5_sel2)\n')
    output_file.write('create(col,"col2",db1.tbl5_sel2)\n')
    output_file.write('load("'+DOCKER_TEST_BASE_DIR+'/data5_selectivity2.csv")\n')
    output_file.write('--\n')
    output_file.write('-- Testing that the data and their indexes are durable on disk.\n')
    output_file.write('shutdown\n')
    # no expected results
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTest46(factTable, dimTable2, dataSizeFact, dataSizeDim2, selectivityFact, selectivityDim2):
    output_file, exp_output_file = data_gen_utils.openFileHandles(46, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- First join test - nested-loop. Select + Join + aggregation\n')   
    output_file.write('-- Performs the join using nested loops\n')
    output_file.write('-- Do this only on reasonable sized tables! (O(n^2))\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT avg(tbl5_fact.col2), sum(tbl5_fact.col3) FROM tbl5_fact,tbl5_dim2 WHERE tbl5_fact.col4=tbl5_dim2.col1 AND tbl5_fact.col2 < {} AND tbl5_dim2.col1<{};\n'.format(int((dataSizeFact/5) * selectivityFact), int(selectivityDim2 * dataSizeDim2)))
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('p1=select(db1.tbl5_fact.col2,null, {})\n'.format(int((dataSizeFact/5) * selectivityFact)))
    output_file.write('p2=select(db1.tbl5_dim2.col1,null, {})\n'.format(int(dataSizeDim2 * selectivityDim2)))
    #output_file.write('print(p1)\n')
    #output_file.write('print(p2)\n')
    output_file.write('f1=fetch(db1.tbl5_fact.col4,p1)\n')
    output_file.write('f2=fetch(db1.tbl5_dim2.col1,p2)\n')
    output_file.write('t1,t2=join(f1,p1,f2,p2,nested-loop)\n')
    output_file.write('col2joined=fetch(db1.tbl5_fact.col2,t1)\n')
    output_file.write('col3joined=fetch(db1.tbl5_fact.col3,t2)\n')
    output_file.write('a1=avg(col2joined)\n')
    output_file.write('a2=sum(col3joined)\n')
    output_file.write('print(a1,a2)\n')
    # generate expected results
    dfFactTableMask = (factTable['col2'] < int((dataSizeFact/5) * selectivityFact))
    dfDimTableMask = (dimTable2['col1'] < int(dataSizeDim2 * selectivityDim2))
    preJoinFact = factTable[dfFactTableMask]
    preJoinDim2 = dimTable2[dfDimTableMask]
    joinedTable = preJoinFact.merge(preJoinDim2, left_on = 'col4', right_on = 'col1', suffixes=('','_right'))
    col2ValuesMean = joinedTable['col2'].mean()
    col3ValuesSum = joinedTable['col3'].sum()
    if (math.isnan(col2ValuesMean)):
        exp_output_file.write('0.00,')
    else:
        exp_output_file.write('{:0.2f},'.format(col2ValuesMean))
    if (math.isnan(col3ValuesSum)):
        exp_output_file.write('0\n')
    else:
        exp_output_file.write('{}\n'.format(col3ValuesSum))

def createTest47(factTable, dimTable2, dataSizeFact, dataSizeDim2, selectivityFact, selectivityDim2):
    output_file, exp_output_file = data_gen_utils.openFileHandles(47, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- First join test - hash. Select + Join + aggregation\n')
    output_file.write('-- Performs the join using hashing\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT avg(tbl5_fact.col2), sum(tbl5_fact.col3) FROM tbl5_fact,tbl5_dim2 WHERE tbl5_fact.col4=tbl5_dim2.col1 AND tbl5_fact.col2 < {} AND tbl5_dim2.col1<{};\n'.format(int((dataSizeFact/5) * selectivityFact), int(selectivityDim2 * dataSizeDim2)))
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('p1=select(db1.tbl5_fact.col2,null, {})\n'.format(int((dataSizeFact/5) * selectivityFact)))
    output_file.write('p2=select(db1.tbl5_dim2.col1,null, {})\n'.format(int(dataSizeDim2 * selectivityDim2)))
    output_file.write('f1=fetch(db1.tbl5_fact.col4,p1)\n')
    output_file.write('f2=fetch(db1.tbl5_dim2.col1,p2)\n')
    output_file.write('t1,t2=join(f1,p1,f2,p2,hash)\n')
    output_file.write('col2joined=fetch(db1.tbl5_fact.col2,t1)\n')
    output_file.write('col3joined=fetch(db1.tbl5_fact.col3,t2)\n')
    output_file.write('a1=avg(col2joined)\n')
    output_file.write('a2=sum(col3joined)\n')
    output_file.write('print(a1,a2)\n')
    # generate expected results
    dfFactTableMask = (factTable['col2'] < int((dataSizeFact/5) * selectivityFact))
    dfDimTableMask = (dimTable2['col1'] < int(dataSizeDim2 * selectivityDim2))
    preJoinFact = factTable[dfFactTableMask]
    preJoinDim2 = dimTable2[dfDimTableMask]
    joinedTable = preJoinFact.merge(preJoinDim2, left_on = 'col4', right_on = 'col1', suffixes=('','_right'))
    col2ValuesMean = joinedTable['col2'].mean()
    col3ValuesSum = joinedTable['col3'].sum()
    if (math.isnan(col2ValuesMean)):
        exp_output_file.write('0.00,')
    else:
        exp_output_file.write('{:0.2f},'.format(col2ValuesMean))
    if (math.isnan(col3ValuesSum)):
        exp_output_file.write('0\n')
    else:
        exp_output_file.write('{}\n'.format(col3ValuesSum))

def createTest48(factTable, dimTable1, dataSizeFact, dataSizeDim1, selectivityFact, selectivityDim1):
    output_file, exp_output_file = data_gen_utils.openFileHandles(48, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Join test 2 - nested-loop. Select + Join + aggregation\n')
    output_file.write('-- Performs the join using nested loops\n')
    output_file.write('-- Do this only on reasonable sized tables! (O(n^2))\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT sum(tbl5_fact.col2), avg(tbl5_dim1.col1) FROM tbl5_fact,tbl5_dim1 WHERE tbl5_fact.col1=tbl5_dim1.col1 AND tbl5_fact.col2 < {} AND tbl5_dim1.col3<{};\n'.format(int(selectivityFact * (dataSizeFact / 5)), int((dataSizeDim1/5) * selectivityDim1)))
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('p1=select(db1.tbl5_fact.col2,null, {})\n'.format(int(selectivityFact * (dataSizeFact / 5))))
    output_file.write('p2=select(db1.tbl5_dim1.col3,null, {})\n'.format(int((dataSizeDim1/5) * selectivityDim1)))
    output_file.write('f1=fetch(db1.tbl5_fact.col1,p1)\n')
    output_file.write('f2=fetch(db1.tbl5_dim1.col1,p2)\n')
    output_file.write('t1,t2=join(f1,p1,f2,p2,nested-loop)\n')
    output_file.write('col2joined=fetch(db1.tbl5_fact.col2,t1)\n')
    output_file.write('col1joined=fetch(db1.tbl5_dim1.col1,t2)\n')
    output_file.write('a1=sum(col2joined)\n')
    output_file.write('a2=avg(col1joined)\n')
    output_file.write('print(a1,a2)\n')
    # generate expected results
    dfFactTableMask = (factTable['col2'] < int(selectivityFact * (dataSizeFact / 5)))
    dfDimTableMask = (dimTable1['col3'] < int((dataSizeDim1/5) * selectivityDim1))
    preJoinFact = factTable[dfFactTableMask]
    preJoinDim1 = dimTable1[dfDimTableMask]
    joinedTable = preJoinFact.merge(preJoinDim1, left_on = 'col1', right_on = 'col1', suffixes=('','_right'))
    col2ValuesSum = joinedTable['col2'].sum()
    col1ValuesMean = joinedTable['col1'].mean()
    if (math.isnan(col2ValuesSum)):
        exp_output_file.write('0,')
    else:
        exp_output_file.write('{},'.format(col2ValuesSum))
    if (math.isnan(col1ValuesMean)):
        exp_output_file.write('0.00\n')
    else:
        exp_output_file.write('{:0.2f}\n'.format(col1ValuesMean))

def createTest49(factTable, dimTable1, dataSizeFact, dataSizeDim1, selectivityFact, selectivityDim1):
    output_file, exp_output_file = data_gen_utils.openFileHandles(49, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- join test 2 - hash. Select + Join + aggregation\n')
    output_file.write('-- Performs the join using hashing\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT sum(tbl5_fact.col2), avg(tbl5_dim1.col1) FROM tbl5_fact,tbl5_dim1 WHERE tbl5_fact.col1=tbl5_dim1.col1 AND tbl5_fact.col2 < {} AND tbl5_dim1.col3<{};\n'.format(int(selectivityFact * (dataSizeFact / 5)), int((dataSizeDim1/5) * selectivityDim1)))
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('p1=select(db1.tbl5_fact.col2,null, {})\n'.format(int(selectivityFact * (dataSizeFact / 5))))
    output_file.write('p2=select(db1.tbl5_dim1.col3,null, {})\n'.format(int((dataSizeDim1/5) * selectivityDim1)))
    output_file.write('f1=fetch(db1.tbl5_fact.col1,p1)\n')
    output_file.write('f2=fetch(db1.tbl5_dim1.col1,p2)\n')
    output_file.write('t1,t2=join(f1,p1,f2,p2,hash)\n')
    output_file.write('col2joined=fetch(db1.tbl5_fact.col2,t1)\n')
    output_file.write('col1joined=fetch(db1.tbl5_dim1.col1,t2)\n')
    output_file.write('a1=sum(col2joined)\n')
    output_file.write('a2=avg(col1joined)\n')
    output_file.write('print(a1,a2)\n')
    # generate expected results
    dfFactTableMask = (factTable['col2'] < int(selectivityFact * (dataSizeFact / 5)))
    dfDimTableMask = (dimTable1['col3'] < int((dataSizeDim1/5) * selectivityDim1))
    preJoinFact = factTable[dfFactTableMask]
    preJoinDim1 = dimTable1[dfDimTableMask]
    joinedTable = preJoinFact.merge(preJoinDim1, left_on = 'col1', right_on = 'col1', suffixes=('','_right'))
    col2ValuesSum = joinedTable['col2'].sum()
    col1ValuesMean = joinedTable['col1'].mean()
    if (math.isnan(col2ValuesSum)):
        exp_output_file.write('0,')
    else:
        exp_output_file.write('{},'.format(col2ValuesSum))
    if (math.isnan(col1ValuesMean)):
        exp_output_file.write('0.00\n')
    else:
        exp_output_file.write('{:0.2f}\n'.format(col1ValuesMean))

def createTest50(factTable, dimTable2, dataSizeFact, dataSizeDim2, selectivityFact, selectivityDim2):
    output_file, exp_output_file = data_gen_utils.openFileHandles(50, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- join test 3 - hashing many-one with larger selectivities.\n')
    output_file.write('-- Select + Join + aggregation\n')
    output_file.write('-- Performs the join using hashing\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT avg(tbl5_fact.col2), sum(tbl5_dim2.col2) FROM tbl5_fact,tbl5_dim2 WHERE tbl5_fact.col4=tbl5_dim2.col1 AND tbl5_fact.col2 < {} AND tbl5_dim2.col1<{};\n'.format(int((dataSizeFact/5) * selectivityFact), int(selectivityDim2 * dataSizeDim2)))
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('p1=select(db1.tbl5_fact.col2,null, {})\n'.format(int((dataSizeFact/5) * selectivityFact)))
    output_file.write('p2=select(db1.tbl5_dim2.col1,null, {})\n'.format(int(dataSizeDim2 * selectivityDim2)))
    output_file.write('f1=fetch(db1.tbl5_fact.col4,p1)\n')
    output_file.write('f2=fetch(db1.tbl5_dim2.col1,p2)\n')
    output_file.write('t1,t2=join(f1,p1,f2,p2,hash)\n')
    output_file.write('col2joined=fetch(db1.tbl5_fact.col2,t1)\n')
    output_file.write('col2t2joined=fetch(db1.tbl5_dim2.col2,t2)\n')
    output_file.write('a1=avg(col2joined)\n')
    output_file.write('a2=sum(col2t2joined)\n')
    output_file.write('print(a1,a2)\n')
    # generate expected results
    dfFactTableMask = (factTable['col2'] < int((dataSizeFact/5) * selectivityFact))
    dfDimTableMask = (dimTable2['col1'] < int(dataSizeDim2 * selectivityDim2))
    preJoinFact = factTable[dfFactTableMask]
    preJoinDim2 = dimTable2[dfDimTableMask]
    joinedTable = preJoinFact.merge(preJoinDim2, left_on = 'col4', right_on = 'col1', suffixes=('','_right'))
    col2ValuesMean = joinedTable['col2'].mean()
    col3ValuesSum = joinedTable['col2_right'].sum()
    if (math.isnan(col2ValuesMean)):
        exp_output_file.write('0.00,')
    else:
        exp_output_file.write('{:0.2f},'.format(col2ValuesMean))
    if (math.isnan(col3ValuesSum)):
        exp_output_file.write('0\n')
    else:
        exp_output_file.write('{}\n'.format(col3ValuesSum))

def createTest51(factTable, dimTable1, dataSizeFact, dataSizeDim1, selectivityFact, selectivityDim1):
    output_file, exp_output_file = data_gen_utils.openFileHandles(51, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- join test 4 - hashing many-many with larger selectivities.\n')
    output_file.write('-- Select + Join + aggregation\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT sum(tbl5_fact.col2), avg(tbl5_dim1.col1) FROM tbl5_fact,tbl5_dim1 WHERE tbl5_fact.col1=tbl5_dim1.col1 AND tbl5_fact.col2 < {} AND tbl5_dim1.col3<{};\n'.format(int(selectivityFact * (dataSizeFact / 5)), int((dataSizeDim1/5) * selectivityDim1)))
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('p1=select(db1.tbl5_fact.col2,null, {})\n'.format(int(selectivityFact * (dataSizeFact / 5))))
    output_file.write('p2=select(db1.tbl5_dim1.col3,null, {})\n'.format(int((dataSizeDim1/5) * selectivityDim1)))
    output_file.write('f1=fetch(db1.tbl5_fact.col1,p1)\n')
    output_file.write('f2=fetch(db1.tbl5_dim1.col1,p2)\n')
    output_file.write('t1,t2=join(f1,p1,f2,p2,hash)\n')
    output_file.write('col2joined=fetch(db1.tbl5_fact.col2,t1)\n')
    output_file.write('col1joined=fetch(db1.tbl5_dim1.col1,t2)\n')
    output_file.write('a1=sum(col2joined)\n')
    output_file.write('a2=avg(col1joined)\n')
    output_file.write('print(a1,a2)\n')
    # generate expected results
    dfFactTableMask = (factTable['col2'] < int(selectivityFact * (dataSizeFact / 5)))
    dfDimTableMask = (dimTable1['col3'] < int((dataSizeDim1/5) * selectivityDim1))
    preJoinFact = factTable[dfFactTableMask]
    preJoinDim1 = dimTable1[dfDimTableMask]
    joinedTable = preJoinFact.merge(preJoinDim1, left_on = 'col1', right_on = 'col1', suffixes=('','_right'))
    col2ValuesSum = joinedTable['col2'].sum()
    col1ValuesMean = joinedTable['col1'].mean()
    if (math.isnan(col2ValuesSum)):
        exp_output_file.write('0,')
    else:
        exp_output_file.write('{},'.format(col2ValuesSum))
    if (math.isnan(col1ValuesMean)):
        exp_output_file.write('0.00\n')
    else:
        exp_output_file.write('{:0.2f}\n'.format(col1ValuesMean))
    
def _perf_test_helper(output_file, dataSizeSelect, selectivity_1, selectivity_2, join_type: str):

    # dataSizeSelect / 5 == range of values.
    upper_bound_1 = int(selectivity_1 * (dataSizeSelect / 5))
    upper_bound_2 = int(selectivity_2 * (dataSizeSelect / 5))

    output_file.write(f'-- join performance test - {join_type} with selectivities {selectivity_1} and {selectivity_2}.\n')
    output_file.write('-- Select + Join + aggregation\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT sum(tbl5_sel1.col1), avg(tbl5_sel2.col2) FROM tbl5_sel1, tbl5_sel2 WHERE tbl5_sel1.col1=tbl5_sel2.col1 AND tbl5_sel1.col1 < {} AND tbl5_sel2.col2<{};\n'.format(upper_bound_1, upper_bound_2))
    output_file.write('--\n')
    output_file.write('--\n')

    output_file.write('p1=select(db1.tbl5_sel1.col1,null, {})\n'.format(upper_bound_1))
    output_file.write('p2=select(db1.tbl5_sel2.col2,null, {})\n'.format(upper_bound_2))
    output_file.write('f1=fetch(db1.tbl5_sel1.col1,p1)\n')
    output_file.write('f2=fetch(db1.tbl5_sel2.col1,p2)\n')

    output_file.write(f't1,t2=join(f1,p1,f2,p2,{join_type})\n')

def create_join_correctness_test(select_table_1, 
                                 select_table_2, 
                                 data_size, 
                                 selectivity_1, 
                                 selectivity_2, 
                                 join_type_1: str, 
                                 join_type_2: str,
                                 test_num):
    """
    This test checks correctness.
    """

    # First join 
    output_file_1, exp_output_file_1 = data_gen_utils.openFileHandles(test_num, TEST_DIR=TEST_BASE_DIR)
    _perf_test_helper(output_file_1, data_size, selectivity_1, selectivity_2, join_type_1)
    output_file_1.write('col1joined=fetch(db1.tbl5_sel1.col1,t1)\n')
    output_file_1.write('col2joined=fetch(db1.tbl5_sel2.col2,t2)\n')
    output_file_1.write('a1=sum(col1joined)\n')
    output_file_1.write('a2=avg(col2joined)\n')
    output_file_1.write('print(a1,a2)\n')

    # Second join
    output_file_2, exp_output_file_2 = data_gen_utils.openFileHandles(test_num + 1, TEST_DIR=TEST_BASE_DIR)
    _perf_test_helper(output_file_2, data_size, selectivity_1, selectivity_2, join_type_2)
    output_file_2.write('col1joined=fetch(db1.tbl5_sel1.col1,t1)\n')
    output_file_2.write('col2joined=fetch(db1.tbl5_sel2.col2,t2)\n')
    output_file_2.write('a1=sum(col1joined)\n')
    output_file_2.write('a2=avg(col2joined)\n')
    output_file_2.write('print(a1,a2)\n')

    # generate expected results
    upper_bound_1 = int(selectivity_1 * (data_size / 5))
    upper_bound_2 = int(selectivity_2 * (data_size / 5))

    pre_join_sel_1 = select_table_1[select_table_1['col1'] < upper_bound_1]
    pre_join_sel_2 = select_table_2[select_table_2['col2'] < upper_bound_2]

    joined_table = pre_join_sel_1.merge(pre_join_sel_2, left_on = 'col1', right_on = 'col1', suffixes=('','_right'))
    col_1_values_sum = joined_table['col1'].sum()
    col_2_values_mean = joined_table['col2_right'].mean()

    if (math.isnan(col_1_values_sum)):
        exp_output_file_1.write('0,')
        exp_output_file_2.write('0,')
    else:
        exp_output_file_1.write('{},'.format(col_1_values_sum))
        exp_output_file_2.write('{},'.format(col_1_values_sum))
    if (math.isnan(col_2_values_mean)):
        exp_output_file_1.write('0.00\n')
        exp_output_file_2.write('0.00\n')
    else:
        exp_output_file_1.write('{:0.2f}\n'.format(col_2_values_mean))
        exp_output_file_2.write('{:0.2f}\n'.format(col_2_values_mean))

    data_gen_utils.closeFileHandles(output_file_1, exp_output_file_1)
    data_gen_utils.closeFileHandles(output_file_2, exp_output_file_2)

def create_join_perf_test(data_size, 
                          selectivity_1, 
                          selectivity_2, 
                          join_type_1: str,                 
                          join_type_2: str,
                          test_num):
    """
    Same as the previous test, but only checks for performance (not correctness).
    """
    # First join 
    output_file_1, exp_output_file_1 = data_gen_utils.openFileHandles(test_num, TEST_DIR=TEST_BASE_DIR)
    _perf_test_helper(output_file_1, data_size, selectivity_1, selectivity_2, join_type_1)
    data_gen_utils.closeFileHandles(output_file_1, exp_output_file_1)
    
    # Second join
    output_file_2, exp_output_file_2 = data_gen_utils.openFileHandles(test_num + 1, TEST_DIR=TEST_BASE_DIR)
    _perf_test_helper(output_file_2, data_size, selectivity_1, selectivity_2, join_type_2)
    data_gen_utils.closeFileHandles(output_file_2, exp_output_file_2)

def createTest52_55(select_table_1, select_table_2, data_size):
    """
    Compare nested-loop with naive-hash for lower selectivities.
    Expect naive-hash to be faster.
    Creates a total of 4 tests.
    """
    test_num = 52
    join_type_1, join_type_2 = "nested-loop", "naive-hash"
    selectivity_1 = selectivity_2 = 0.1
    create_join_correctness_test(
        select_table_1,
        select_table_2,
        data_size,
        selectivity_1,
        selectivity_2,
        join_type_1,
        join_type_2,
        test_num
    )

    create_join_perf_test(
        data_size,
        selectivity_1,
        selectivity_2,
        join_type_1,
        join_type_2,
        test_num + 2
    )

def createTest56_59(select_table_1, select_table_2, data_size):
    """
    Compare naive-hash with grace-hash for higher selectivities.
    Expect grace-hash to be faster.
    Creates a total of 4 tests.
    """
    test_num = 56
    join_type_1, join_type_2 = "naive-hash", "grace-hash"
    selectivity_1 = selectivity_2 = 0.8
    create_join_correctness_test(
        select_table_1,
        select_table_2,
        data_size,
        selectivity_1,
        selectivity_2,
        join_type_1,
        join_type_2,
        test_num
    )

    create_join_perf_test(
        data_size,
        selectivity_1,
        selectivity_2,
        join_type_1,
        join_type_2,
        test_num + 2
    )

def generateMilestoneFourFiles(dataSizeFact, dataSizeDim1, dataSizeDim2, dataSizeSelect, zipfianParam, numDistinctElements, randomSeed=47):
    np.random.seed(randomSeed)
    factTable, dimTable1, dimTable2, selectTable1, selectTable2 = generateDataMilestone4(dataSizeFact, dataSizeDim1, dataSizeDim2, dataSizeSelect, zipfianParam, numDistinctElements)  
    createTest45()
    # test many to 1 joins
    createTest46(factTable, dimTable2, dataSizeFact, dataSizeDim2, 0.15, 0.15)
    createTest47(factTable, dimTable2, dataSizeFact, dataSizeDim2, 0.15, 0.15)
    # test many to many joins
    createTest48(factTable, dimTable1, dataSizeFact, dataSizeDim1, 0.15, 0.15)
    createTest49(factTable, dimTable1, dataSizeFact, dataSizeDim1, 0.15, 0.15)
    # test both joins with much larger selectivities. This should mostly test speed.
    createTest50(factTable, dimTable2, dataSizeFact, dataSizeDim2, 0.8, 0.8)
    createTest51(factTable, dimTable1, dataSizeFact, dataSizeDim1, 0.8, 0.8)

    assert(len(selectTable1) == len(selectTable2))
    createTest52_55(selectTable1, selectTable2, len(selectTable1))
    createTest56_59(selectTable1, selectTable2, len(selectTable1))

def main(argv):
    global TEST_BASE_DIR
    global DOCKER_TEST_BASE_DIR

    dataSizeFact = int(argv[0])
    dataSizeDim1 = int(argv[1])
    dataSizeDim2 = int(argv[2])
    dataSizeSelect = int(argv[3])
    if len(argv) > 7:
        randomSeed = int(argv[4])
        zipfianParam = np.double(argv[5])
        numDistinctElements = int(argv[6])
        TEST_BASE_DIR = argv[7]
		
        if len(argv) > 8:
            DOCKER_TEST_BASE_DIR = argv[8]

    elif len(argv) > 6:
        randomSeed = argv[4]
        zipfianParam = np.double(argv[5])
        numDistinctElements = int(argv[6])
    elif len(argv) > 4:
        randomSeed = int(argv[4])
        zipfianParam = 1.0
        numDistinctElements = 50
    else:
        randomSeed = 47
        zipfianParam = 1.0
        numDistinctElements = 50

    generateMilestoneFourFiles(dataSizeFact, dataSizeDim1, dataSizeDim2, dataSizeSelect, zipfianParam, numDistinctElements, randomSeed=randomSeed)

if __name__ == "__main__":
    main(sys.argv[1:])
    
