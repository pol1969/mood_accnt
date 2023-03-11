#!/usr/bin/env python3
"""
Author : pol1969 <podberezsky1969@gmail.com>
Date   : 2023-03-06
Purpose: Python automotion in MOOD
"""

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.sql import operators, extract
from sqlalchemy import and_
from datetime import date
import click

import pdb
# --------------------------------------------------

dict = {34:'Подберезский П В',83:'Духнов А М',
        99:'Синицын П А',8582:'Буев В С'}

def print_doctor(doctor,year,month,session,oper):
    """Делает запрос времени операций за месяц по врачу"""

    q = session.query(
           func.date(oper.AN_DATEWYP).label('day'),
               (func.sum((func.time_to_sec(oper.END_OPER)-
                func.time_to_sec(oper.START_OPER))/60)).
                    label('time')).filter(
                    func.year(oper.AN_DATEWYP)==year).filter(
                    func.month(oper.AN_DATEWYP)==month).filter(
                            (oper.AN_WRAISP == doctor) |
                            (oper.AN_WRAASS1 == doctor) |
                            (oper.AN_WRAASS2 == doctor) |
                            (oper.AN_WRAASS3 == doctor)).group_by(
                                    func.date(oper.AN_DATEWYP))

#    print(len(q.all()))

    [print(i.day, i.time) for i in q.all()]





@click.command()
@click.option('-m','--month', default=date.today().month,
              help='Номер месяца')
@click.option('-y','--year', default=date.today().year,
              help='Номер года')                                                                                                                        #@click.option('-d','--doctor',default=34,
#              help='Код врача: 34 Подберезский,83 Духнов, 99 Синицын, 8582 Буев ')
def main(month,year):
    """Подсчет времени операций в минутах за месяц по дням"""

    Base = automap_base()
    engine = create_engine('mysql+pymysql://pol:19691319@192.168.31.1/moodMS')
    Base.prepare(engine, reflect=True)

    session = Session(engine)

    oper = Base.classes.oper

    for i in dict:
        print()
        click.echo(dict[i])
        print_doctor(i,year,month,session,oper)



# --------------------------------------------------
if __name__ == '__main__':
    main()
