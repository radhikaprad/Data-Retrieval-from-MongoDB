import sys,os
import csv

import pymongo
from pymongo import MongoClient
from bson.code import Code
import pandas as pd


def join_funtion():
    country_file="d:/Country.csv"
    match_file="d:/Match_results.csv"
    players_file="d:/Players.csv"
    players_goals="d:/Player_Assists_Goals.csv"
    player_cards="d:/Player_Cards.csv"
    world_cup="d:/WorldCup_History.csv"


    countries = dict()
    match=dict()
    players = dict()
    playerscard = dict()
    match1=dict()
    playerassistgoals = dict()
    country_player_cards = dict()
    players_playersasssistgoals = dict()
    players_playercard = dict()
    country_players=dict()
    with open(player_cards, 'rU') as csvfilepc:
        datapc = csv.reader(csvfilepc)
        for rowpc in datapc:
            playerscard[rowpc[0]] = rowpc

    with open(players_goals , 'rU') as csvfilepg:
        datapg = csv.reader(csvfilepg)
        for rowpg in datapg:
            playerassistgoals[rowpg[0]] = rowpg


    with open(players_file, 'rU') as csvfilep:
        datap=csv.reader(csvfilep)
        for rowp in datap:
            players[rowp[0]]=rowp

    with open(country_file, 'rU') as csvfile:
        data = csv.reader(csvfile)
        for row in data:
            countries[row[0]] = row

    with open(players_file, 'rU') as csvfilep:
        datap = csv.reader(csvfilep)
        with open('d:/mongo/country_players.csv', 'wb') as csvfile01:
            data01 = csv.writer(csvfile01)
            for rowp in datap:
                row = countries[rowp[5]]
                #rowp = players[rowp[0]]
                value = [row[0],row[1],row[3],rowp[2],rowp[3],rowp[4],rowp[6],rowp[8],rowp[10],rowp[0],row[2],row[4]]
                #playerid_cp=rowp[0]
                data01.writerow(value)

    with open('d:/mongo/country_players.csv', 'rU') as csvfilecp:
        filteredcp = (line.replace('\r', '') for line in csvfilecp)
        datacp = csv.reader(filteredcp)
        for rowcp in datacp:

            country_players[rowcp[9]] = rowcp #player id is key and rest row is value in country _players
    players_with_cards=dict()
    with open(player_cards, 'rU') as csvfilepc:
        datapc = csv.reader(csvfilepc)
        for rowpc in datapc:
            players_with_cards[rowpc[0]] = rowpc

    with open('country_player_cards.csv', 'wb') as csvfile02:
        data02 = csv.writer(csvfile02)
        for playerId in country_players:
            rowcpc=country_players[playerId]
            if playerId in player_cards:
                rowpc=player_cards[playerId]
            else:
                rowpc=[playerId,0,0]
            value = [rowcpc[0], rowcpc[1], rowcpc[2], rowcpc[3], rowcpc[4], rowcpc[5], rowcpc[6], rowcpc[7], rowcpc[8], rowpc[1], rowpc[2], rowcpc[9], rowcpc[10], rowcpc[11]]
            data02.writerow(value)



    with open('d:/mongo/country_player_cards.csv', 'rU') as csvfilecpc:
        filteredcpc = (line.replace('\r', '') for line in csvfilecpc)
        datacpc = csv.reader(filteredcpc)
        for rowcpc in datacpc:
            country_player_cards[rowcpc[11]] = rowcpc # player id is key
    players_with_goals=dict()
    with open(players_goals, 'rU') as csvfilepg:
        datapg=csv.reader(csvfilepg)
        for rowpg in datapg:
            players_with_goals[rowpg[0]]=rowpg

    with open('country_player_cards_goals.csv', 'wb') as csvfile03:
        data03 = csv.writer(csvfile03)
        for playerId in country_player_cards:
            rowcpcg=country_player_cards[playerId]
            if playerId in players_goals:
                rowpg=players_goals[playerId]
            else:
                rowpg=[playerId,0,0]
            value = [rowcpcg[0], rowcpcg[1], rowcpcg[2], rowcpcg[3], rowcpcg[4], rowcpcg[5], rowcpcg[6], rowcpcg[7],
                     rowcpcg[8], rowcpcg[9], rowcpcg[10], rowpg[1], rowpg[2], rowcpcg[11], rowcpcg[12], rowcpcg[13]]
            data03.writerow(value)

    country_player_cards_goals=dict()
    with open('d:/mongo/country_player_cards_goals.csv', 'rU') as csvfilecpcg:
        datacpcg=csv.reader(csvfilecpcg)
        for rowcpcg in datacpcg:
            country_player_cards_goals[rowcpcg[13]]=rowcpcg
    #country_player_cards_goals has playerId as key and rest as value
    #rowcpcg[0] is country and need to be compared with rowwc[2]
    cupwinner=dict()
    country_win=dict()
    with open(world_cup, 'rU') as csvfilewc:
        datawc=csv.reader(csvfilewc)
        for rowwc in datawc:
            country_win[rowwc[2]]=rowwc


    with open(world_cup, 'rU') as csvfilewc:
        datawc=csv.reader(csvfilewc)
        for rowwc in datawc:
            cupwinner[rowwc[2]]=rowwc

    with open('Country.csv', 'wb') as csvfile04:
        data04 = csv.writer(csvfile04)
        for playerId in country_player_cards_goals:
            rowcpcgw=country_player_cards_goals[playerId]
            country=rowcpcgw[0]
            if country in country_win:
                rowwc=country_win[country]

            else:
                rowwc=['no', 'no', 'no']
            value = [rowcpcgw[0], rowcpcgw[1], rowcpcgw[2], rowcpcgw[3], rowcpcgw[4], rowcpcgw[5], rowcpcgw[6], rowcpcgw[7],rowcpcgw[8], rowcpcgw[9], rowcpcgw[10], rowcpcgw[11], rowcpcgw[12], rowwc[0], rowwc[1], rowcpcgw[13], rowcpcgw[14], rowcpcgw[15]]
            #print value
            data04.writerow(value)

    with open(match_file, 'rU') as csvfilem:
        datam = csv.reader(csvfilem)
        for rowm in datam:
            match[rowm[3]] = rowm
            match1[rowm[7]]=rowm

    with open(match_file, 'rU') as csvfilem:
        datam = csv.reader(csvfilem)
        with open('d:/mongo/stadium.csv', 'wb') as csvfile11:
            data11 = csv.writer(csvfile11)
            for rowm in datam:
                row = match[rowm[3]]
                value = [rowm[7], rowm[8], rowm[1], rowm[3], rowm[4], rowm[5], rowm[6]]
                data11.writerow(value)




    client=MongoClient('127.0.0.1',27017)
    db=client.mongodb


    with open('d:/mongo/Country.csv', 'rU') as csvfilefinal1:
        datafinal1=csv.reader(csvfilefinal1)
        for rowcpcgw1 in datafinal1:
            Country=dict()
            Country['Country_name']=rowcpcgw1[0]
            Country['Population']=rowcpcgw1[1]
            Country['Manager']=rowcpcgw1[2]
            Country['PlayerFirstName']=rowcpcgw1[3]
            Country['PlayerLastName']=rowcpcgw1[4]
            Country['PlayerDOB']=rowcpcgw1[5]
            Country['PlayerHeight']=rowcpcgw1[6]
            Country['PlayerPosition']=rowcpcgw1[7]
            Country['PlayerIsCaptain']=rowcpcgw1[8]
            Country['PlayerYellowCards']=rowcpcgw1[9]
            Country['PlayerRedCards']=rowcpcgw1[10]
            Country['PlayerGoals']=rowcpcgw1[11]
            Country['PlayerAssist']=rowcpcgw1[12]
            Country['Year']=rowcpcgw1[13]
            Country['Host']=rowcpcgw1[14]
            Country['No_of_WorldCup']=rowcpcgw1[16]
            Country['Capital']=rowcpcgw1[17]
            db.Countries.insert_one(Country)




    with open('d:/mongo/stadium.csv', 'rU') as csvfilefinals:
        datafinals=csv.reader(csvfilefinals)
        for rows in datafinals:
            Stadium=dict()
            Stadium['Stadium']=rows[0]
            Stadium['HostCity']=rows[1]
            Stadium['Date']=rows[2]
            Stadium['Team1']=rows[3]
            Stadium['Team2']=rows[4]
            Stadium['Team1Score']=rows[5]
            Stadium['Team2Score']=rows[6]
            db.Stadiums.insert_one(Stadium)

#to print list of the countries won
    print "List of courties which won world-cup"
    cursor1= db.Countries.find({"Year":{"$ne":'no'}},{"Country_name":1, '_id':0}).distinct("Country_name")
    for document1 in cursor1:
        print document1




    print "list of countries with the number of world cups in descending order"
    reducer = Code("""
                function(curr, result){
                 result.No_of_WorldCup = curr.No_of_WorldCup;
                }
               """)
#to print list of countries won world cup with number in decsending order
    #db.Countries.find({"Year":{"$ne":'no'}},{"Country_name":1, "No_of_WorldCup":1,'_id':0}).sort("No_of_WorldCup", pymongo.DESCENDING).distinct("Country_name",{'Country_name':1, 'No_of_WorldCup':1,'_id':0})
                          #group(key={"x":1}, condition={}, initial={"count": 0}, reduce=reducer)
    cursor2= db.Countries.group(key={"Country_name":1},condition={ "Year":{"$ne":'no'}},initial= { 'No_of_WorldCup' : 0 },reduce= reducer)
    db.temp.insert(cursor2)
    cursor2 = db.temp.find({},{'Country_name':1,'No_of_WorldCup':1,'_id':0}).sort("No_of_WorldCup", pymongo.DESCENDING)
    for document2 in cursor2:
        print document2
    db.temp.drop()
#list stadium and matches hosted by the stadium



#PRINT Capital of country in increasing order of population with population>100
    print "list of capitals with population>100"
    reducer = Code("""
                function(curr, result){
                 result.Population = curr.Population;
                }
               """)
    cursor3= db.Countries.group(key={"Capital":1},condition={ "Population":{"$gt":100}},initial= { 'Population' : 0 },reduce= reducer)
        #db.Countries.find({"Population":{"$gt":100}},{"Capital":1, "Population":1,'_id':0}).sort("Population", pymongo.ASCENDING)
    db.temp.insert(cursor3)
    cursor3 = db.temp.find({},{'Capital':1,'Population':1,'_id':0}).sort("Population", pymongo.ASCENDING)

    for document3 in cursor3:
        print document3

    db.temp.drop()



#print name of host stadium where team score>4
    print "List of teams with scores greater that 4"
    cursor4= db.Stadiums.find({ "$or": [ { "Team1Score": { "$gt": 4 } }, { "Team2Score": { "$gt": 4 }} ] },{"Team1Score":1, "Team2Score":1, "Stadium":1, "HostCity":1,'_id':0})
    for document4 in cursor4:
        print document4

#list cities with stadium starting with "Estadio"
    print "Stadiums starting with Estadio"
    cursor5= db.Stadiums.find({"Stadium":{'$regex': "^'Estadio"}},{"HostCity":1, "Stadium":1,'_id':0})
    for document5 in cursor5:
        print document5

    print "List of stadiums with number of matches"
    reducer22 = Code("""
               function(curr, result){
                 result.No_of_Match=result.No_of_Match+1;
               }
               """)
    cursor22=db.Stadiums.group(key={"HostCity":1}, condition={}, initial={"No_of_Match": 0}, reduce=reducer22)
    for document22 in cursor22:
        print document22
#first name, last name, dob of players with height>198
    print "Players Details with height greater than 198"
    reducer1 = Code("""
                function(curr, result){
                 result.PlayerHeight = curr.PlayerHeight;
                }
               """)
    cursor11= db.Countries.group(key={"PlayerFirstName":1, "PlayerLastName":1, "PlayerDOB":1},condition={ "PlayerHeight":{"$gt":198}},initial= { 'PlayerHeight' : 0 },reduce= reducer1)
    for document11 in cursor11:
        print document11



if __name__=='__main__':
    join_funtion()
