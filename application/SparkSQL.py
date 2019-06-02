import pandas as pd
import numpy as np
import scipy


import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.colors as colors

from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


from sklearn.feature_extraction.text import CountVectorizer 

import string
from nltk.corpus import stopwords

import plotly
import plotly.plotly as py
import plotly.graph_objs as go
import colorlover as cl

import findspark
findspark.init()

import pyspark
from pyspark.sql import SQLContext

import argparse



def pie_chart_brand(data):
    '''
    pie chart for brands
    '''
    pie_chart=data.groupby('Brand_name').agg({'fileIndex':'count'})
    pie_chart.sort_values(by='fileIndex',inplace=True,ascending=False)
    pie_chart=pie_chart.iloc[:20]
    
    bupu = cl.scales['9']['seq']['BuPu']
    
    data_g = []
    
    tr_p = go.Pie(
        labels = pie_chart.index,
        values = pie_chart['fileIndex'],
        hoverinfo='label+percent+value', textinfo='label+percent',
        textfont=dict(size=15),
        marker=dict(colors= cl.interp( bupu, 20 ),
                    line=dict(color='#000000', width=1))
    )
    data_g.append(tr_p)
    layout = go.Layout(title="Top20 most popular brands",titlefont=dict(size=35))
    fig = go.Figure(data=data_g, layout=layout)
    plotly.offline.plot(fig,filename='brands.html')
    

def pie_chart_product(data):
    '''
    pie chart for products
    '''
    pie_chart=result_df.groupby(['Product_name','Brand_name']).agg({'fileIndex':'count'})
    pie_chart.reset_index(inplace=True)
    pie_chart.sort_values(by='fileIndex',inplace=True,ascending=False)
    pie_chart=pie_chart.iloc[:20]
    
    bupu = cl.scales['9']['seq']['BuPu']
    
    data_g = []
    tr_p = go.Pie(
        labels = pie_chart['Product_name'],
        values = pie_chart['fileIndex'],
        text=pie_chart['Brand_name'],
        hoverinfo='text+label+value+percent', textinfo='label+percent',
        textfont=dict(size=13),
        marker=dict(colors= cl.interp( bupu, 20 ),
                    line=dict(color='#000000', width=1))
    )
    data_g.append(tr_p)
    layout = go.Layout(title="Top20 most popular products",titlefont=dict(size=35))
    fig = go.Figure(data=data_g, layout=layout)
    plotly.offline.plot(fig,filename='products.html')
    


def boxplot_rating_brand(data):
    data_g = []
    
    brand_rating=data.groupby('Brand_name').agg({'rating':np.median,'fileIndex':'count'})
    brand_rating.sort_values(by='rating',inplace=True,ascending=False)
    brand_rating=brand_rating[brand_rating['fileIndex']>0]  #select the brands having more than 5 reviews
     

    for col in brand_rating.index:
            data_g.append(go.Box(y=data[data['Brand_name'] == col]['rating'], name=col, showlegend=False))
    layout = go.Layout(title="Boxplot of ratings for each brand",titlefont=dict(size=35))
    fig = go.Figure(data=data_g, layout=layout)
            
    plotly.offline.plot(fig,filename='boxplot of ratings.html')
    

def scatter_review(data):
    colorscale = [[0, '#FAEE1C'], [0.33, '#F3558E'], [0.66, '#9C1DE7'], [1, '#581B98']]
    trace1 = go.Scatter(
    y = data['rating'],
    x = data['Brand_name'],
    mode='markers',
    text=data['review'],

    #textfont=dict(size=13),
    marker=dict(
        size='10',
        color = data['rating'], #set color equal to a variable
        colorscale=colorscale,
        showscale=True
        )
    )
    data_g= [trace1]
    plotly.offline.plot(data_g,filename='scatter plot for reviews.html')
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='function to ')

    parser.add_argument('-a', '--age', type=str, choices=['35-44', '18-24', '45-54', 'over 54', '25-34', '13-17'],\
                     help="select age range, if not set, select all")

    parser.add_argument('-e', '--eyecolor', type=str, choices=['Brown', 'Blue', 'Hazel', 'Green', 'Gray'],\
                     help="select eye color, if not set, select all")

    parser.add_argument('-c', '--haircolor', type=str, choices=['Brunette', 'Auburn', 'Black', 'Red', 'Gray', 'Blonde'],\
                     help="select hair color, if not set, select all")

    parser.add_argument('-t', '--skintone', type=str, choices=['Olive', 'Light', 'Medium', 'Fair', 'Porcelain', 'Deep', 'Tan', 'Dark', 'Ebony'],\
                     help="select skin tone, if not set, select all")

    parser.add_argument('-s', '--skintype', type=str, choices=['Normal', 'Combination', 'Dry', 'Oily'], \
                     help="select skin typei, if not set, select all")

    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-k', '--key', help='Input keyword', required=True)


    args = parser.parse_args()


    sql_str = 'SELECT * FROM joined WHERE review REGEXP \'{}\''.format(args.key) 
    restr = []
    if args.age != None:
        restr.append('age=\'{}\''.format(args.age))
    if args.eyecolor != None:
        restr.append('eyeColor=\'{}\''.format(args.eyecolor))
    if args.haircolor != None:
        restr.append('hairColor=\'{}\''.format(args.haircolor))
    if args.skintone != None:
        restr.append('skinTone=\'{}\''.format(args.skintone))
    if args.skintype != None:
        restr.append('skinType=\'{}\''.format(args.skintype))

    if len(restr) != 0:
        restrictions = ' AND '.join(restr)
        sql_str = sql_str + ' AND ' + restrictions

    print(sql_str)


    sc = pyspark.SparkContext()
    sqlContext = SQLContext(sc)

    print("===== init finished =====")
    joined_spark=sqlContext.read.csv("/Users/xy/Desktop/BIG DATA MGMT SYSTM/Project/joined_data.csv",
                                       header=True, inferSchema=True)

    print("===== load finished =====")
    joined_spark.createOrReplaceTempView('joined')
    result=sqlContext.sql(sql_str)
    result_df = result.toPandas()
    result_df['rating']=result_df['rating'].astype(int)

    print("===== select finished =====")
    pie_chart_brand(result_df)
    pie_chart_product(result_df)
    boxplot_rating_brand(result_df)
    scatter_review(result_df)
    print("===== plot finished =====")

