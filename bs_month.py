#!/usr/bin/env python3
"""
Author : pol1969 <podberezsky1969@gmail.com>
Date   : 2021-10-27
Purpose: Python automotion in MOOD
"""

import argparse
import datetime as dt
from calendar import monthrange
from bs4 import BeautifulSoup
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.sql import operators, extract
from sqlalchemy import and_
from statistics import mean
import pickle

import pdb
# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description='Эта программа для составления отчета по базам МООД',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-y',
                        '--year',
                        help='Год составления отчета, по умолчанию текущий',
                        metavar='year',
                        type=int,
                        default=dt.datetime.now().year)
    parser.add_argument('-m',
                        '--month',
                        help='Месяц составления отчета,по умолчанию предыдущий',
                        metavar='month',
                        type=int,
                        default=dt.datetime.now().month)
    parser.add_argument('-o',
                        '--otd',
                        help='Отделение (30 - 1ХОО, 31 - 2ХОО, 40 - 3ХОО, 32 - 4ХОО, 34 - 6ХОО)',
                        metavar='otd',
                        choices =[40,30,34,31,32],
                        type=int,
                        default=40)
 
    parser.add_argument('-q',
                        '--qwart',
                        help='Отчет за месяцы с первого по q',
                        metavar='qwart',
                        type=int,
                        default=0)


    args = parser.parse_args()

    if args.year < 2000 or args.year >3000:
        parser.error('Год должен быть в пределах от 1995 до 3000')

    if args.month == 0:
        args.month=12
        args.year=args.year-1


    if args.month < 0 or args.month  >12:
        parser.error('Число месяца должно быть от 1 до 12')

    if args.qwart < 0 or args.qwart >12:
        parser.error('Месяц квартала д. б. между 1 и 12')

    return args


# --------------------------------------------------
def main():
    """ Отчет по выписанным пациентам """

    args = get_args()
    year = args.year
    month = args.month
    otd = args.otd
    qwart = args.qwart

#    print(f'Год = {year}')
#    print(f'Месяц = {month}')
#    print(f'Отделение = {otd}')
#    print(f'Месяцы с первого по  = {qwart}')

    
    Base = automap_base()
    engine = create_engine('mysql+pymysql://pol:19691319@192.168.31.1/moodMS')
    Base.prepare(engine, reflect=True)
    
    session = Session(engine)

    ills = Base.classes.ills
    kartotek = Base.classes.kartotek
    oper = Base.classes.oper
#       print(dir(oper))
    class_pmu = Base.classes.class_pmu
#    view_ills = Base.classes.view_ills

    def get_status_op(an_nomiss):
        q = session.query(oper,class_pmu).filter(oper.AN_NOMISS==an_nomiss)
        q = q.join(class_pmu,oper.AN_CLASS_OPER == class_pmu.code).first()
        if q[1].additional_info is None:
            return 0
        return (int(q[1].additional_info))

    def get_status_video(an_nomiss):
        q = session.query(oper,class_pmu).filter(oper.AN_NOMISS==an_nomiss)
        q = q.join(class_pmu,oper.AN_CLASS_OPER == class_pmu.code).first()
        if q[1].video is None:
            return 0
        return (int(q[1].video))


    second_date = dt.date(year, month,monthrange(year,month)[1])
    
    if qwart==0:
        first_date = dt.date(year, month,1)
    else:
        first_date = dt.date(year, 1, 1)
        second_date = dt.date(year, qwart, monthrange(year,qwart)[1])


#    print(first_date,second_date) 
    tbl = session.query(ills).filter(ills.I_OTDELEN==otd,
        ills.I_DATEOU_O.between(first_date, second_date))

#    print(tbl) 
#    [print(i.I_EPIKRIZ) for i in tbl.first()]

    pmu = session.query(class_pmu).filter(class_pmu.additional_info >=1)

#    print(dir(pmu))
#    [print(i) for i in pmu]

    

    html = tbl.all()
    # список операций
    op = []

    #список номеров операций
    nmb_op_list = []

    # список непрооперированных
    not_op = []

    # список прооперированных
    op_man = set() 

    # список повторных операций
    op_dubl =[]

    # предоперационный койкодень
    pkd = []
    
    #общий койкодень
    kd = []

    #сложные операции койкдень
    kd_sl = []

    #вто операции койкодень
    kd_vto = []

    #краткосрочное пребывание койкодень
    kd_kratko = []

    kd_video = []

    for i in html:
        #     pdb.set_trace()
        try:
            bsObj = BeautifulSoup(i.I_EPIKRIZ, 'html.parser')
            # ФИО  пациепнта
            name = bsObj.findAll('div')[6].get_text()

            # дата поступления
            data_post = bsObj.findAll('div')[12].get_text()[10:20]
            data_post = dt.datetime.strptime(data_post,'%Y-%m-%d').date()

            # дата выписки
            data_out = bsObj.findAll('div')[13].get_text()[9:19]
            data_out = dt.datetime.strptime(data_out,'%Y-%m-%d').date()

            #общий койкодень
            days = (data_out-data_post).days
            kd.append(days)
            if days <=4:
                kd_kratko.append(days)

            c = bsObj.findAll(text='Операция:')
#            print(c)
            if (c):
#                op_man.append(name)
                if (len(c)>1):
                    op_dubl.append(name)
                i = 0    
                for n in c:
                    # счетчик операций
                    i += 1
                    # получить дату операции:
                    data_op = n.parent.parent.parent.get_text()
                    nmb_op = data_op
                    data_op = data_op[data_op.find(' от ')+5:(data_op.find(' от ')+15)]
                    nmb_op =  nmb_op[nmb_op.find('№')+1:(nmb_op.find('№')+7)]

                    data_op = dt.datetime.strptime(data_op, '%d %m %Y').date()

                    # дата операции должна совпадать с интервалом госпитализации
                    if (data_op >= data_post and data_op <= data_out):

                        nmb_op_list.append(int(nmb_op))
                        
                        if get_status_op(int(nmb_op))==1:
                            kd_sl.append((data_out-data_post).days)

                        if get_status_op(int(nmb_op))==2:
                            kd_vto.append((data_out-data_post).days)

                        if get_status_video(int(nmb_op))==1:
                            kd_video.append((data_out-data_post).days)                   
                        # прооперированные - множество
                        op_man.add(name)
                        # операции - список
                        op.append(n.next_element)
                        # считаем пкд по первой операции
                        if i == 1:
                            pkd.append((data_op-data_post).days)

#                        print(name, n.next_element, i)



#                    print(f"{name:<40},{len(c)}, {data_op},{data_post},{data_out},{(data_op-data_post).days}") 

            else:
#                print(name)
                not_op.append(name) 

        except:
            print('Ошибка запроса объекта BeautifulSoup')

#    print('Длина списка номеров операций',len(set(nmb_op_list))) 
#    print(nmb_op_list)
#    [print(get_status_op(i)) for i in nmb_op_list ]
#    print("Сложных операций ", len(kd_sl)," койкодень ",sum(kd_sl))
#    print("ВТО операций ",len(kd_vto)," койкодень ", sum(kd_vto))


#    jupyter_nmb_list = []
#    with open('listfile.data', 'rb')as filehandle:
#        jupyter_nmb_list = pickle.load(filehandle)

#   print('Длина списка номеров операций jupyter',len(set(jupyter_nmb_list))) 
#    print(jupyter_nmb_list)

#    print('Разница',set(nmb_op_list) - set(jupyter_nmb_list))

   
    #пациенты, поступившие и выписанные в интервале от и до
    #(select sum(DATEDIFF(AN_DATEEND,AN_DATEBEGIN)) 
    #		from view_ills 
    #	where 	AN_DATEBEGIN>= @ot and AN_DATEBEGIN<= @do  and 
    #			AN_DATEEND>= @ot and AN_DATEEND<= @do 	
    #			and DV_OTDEL= @otd 
    #) as r14_1,

    #select 
    # `ills`.`AN_AMBKART` AS `AN_AMBKART`,
    #  `kartotek`.`K_BITHDAY` AS `K_BITHDAY`,
    #   `kartotek`.`K_DEAD` AS `K_DEAD`,
    #    `ills`.`ISHOD_ZAB` AS `ISHOD_ZAB`,
    #     `ills`.`I_OTDINPUT` AS `I_OTDINPUT`,
    #      `ills`.`I_DATEIN_O` AS `I_DATEIN_O`,
    #       `ills`.`I_DATEOU_O` AS `I_DATEOU_O`,
    #        `ills`.`I_OTDELEN` AS `I_OTDELEN`,
    #         `ills`.`AN_NOMISS` AS `AN_NOMISS` 
    #         from 
    #         ills left join  `kartotek` on `ills`.`AN_AMBKART` =  `kartotek`.`AN_AMBKART`


    print('суммарный койкодень по запросу')

    


    print('From ',first_date,' To ',second_date) 
    print()
    print('------ПО ВЫПИСКАМ----------')
    print('Выписано по выпискам',len(html))
    print('Сделано операций по выпискам',len(op))
    print('Прооперировано людей по выпискам',len(op_man))
    print('Предоперационный койкодень по выпискам',mean(pkd))
    print('Общий койкодень по выпискам', sum(kd))
    print('Средний койкодень по выпискам', mean(kd))
    print("Сложных операций ", len(kd_sl)," койкодень ",sum(kd_sl))
    print("ВТО операций ",len(kd_vto)," койкодень ", sum(kd_vto))
    print("Процент сложных и ВТО: ",(len(kd_sl)+len(kd_vto))/len(op)*100)
    print("Видеоассистированных операций ",len(kd_video)," койкодень ", sum(kd_video))
    print("Процент видеоассистированных ",len(kd_video)/len(op)*100)
    print("Краткосрочное пребывание ",len(kd_kratko)," койкодень ", sum(kd_kratko))
    print()


    print('Не прооперированы по выпискам',len(not_op))
    print('Повторные операции:')
    [print(i) for i in op_dubl]
    


    print()
    print('------------ПО БАЗАМ--------')
    print(f'Выписано {tbl.count()} пациентов')


#    print(tbl.AN_AMBKART)
#    [print(i.AN_NOMISS) for i in tbl.all()]
    #q = session.query(
    #        ills,kartotek,oper).filter(
    #                ills.I_OTDELEN==otd,
    ##                ills.I_DATEOU_O.between(first_date,second_date)).filter(
    #                ills.AN_AMBKART == kartotek.AN_AMBKART).filter(
    #                ills.AN_AMBKART == oper.AN_AMBKART).filter(
    #                and_(
    #                oper.AN_DATEWYP >= ills.AN_DATEWYP,
    #                oper.AN_DATEWYP <= ills.I_DATEOU_O))
    q = session.query(ills,oper).filter(
            ills.I_OTDELEN==otd,ills.I_DATEOU_O.between(first_date,second_date))
    q = q.join(oper, ills.AN_AMBKART == oper.AN_AMBKART)
    q = q.filter(
         and_(   
         func.date(oper.AN_DATEWYP) >= func.date(ills.AN_DATEWYP),
         func.date(oper.AN_DATEWYP) <= func.date(ills.I_DATEOU_O)))

    nmb_oper_list_base = []
    for i in q.all():
#        print(i.oper.AN_NOMISS)
        nmb_oper_list_base.append(i.oper.AN_NOMISS)

#    print(nmb_oper_list_base)

    print('Разница уник операции по выпискам',set(nmb_op_list) - set(nmb_oper_list_base))

    print('Разница уник операции по базам',set(nmb_oper_list_base ) - set(nmb_op_list))
    
    print(f"Число операций {q.count()}")

#    q = session.query(
#            ills,kartotek,oper).filter(
#                    ills.I_OTDELEN==otd,
#                    ills.I_DATEOU_O.between(first_date,second_date)).filter(
#                    ills.AN_AMBKART == kartotek.AN_AMBKART).filter(
#                    ills.AN_AMBKART == oper.AN_AMBKART).filter(
#                    and_(
#                    oper.AN_DATEWYP >= ills.AN_DATEWYP,
#                    oper.AN_DATEWYP <= ills.I_DATEOU_O)).group_by(ills.AN_NOMISS)
    p = q.group_by(ills.AN_NOMISS)
 
    print(f"Число оперированных {p.count()}")


    
    q = session.query(
            ills,kartotek,oper,func.count(oper.AN_CLASS_OPER).label('cnt')).filter(
                    ills.I_OTDELEN==otd,
                    ills.I_DATEOU_O.between(first_date,second_date)).filter(
                    ills.AN_AMBKART == kartotek.AN_AMBKART).filter(
                    ills.AN_AMBKART == oper.AN_AMBKART).filter(
                    and_(
                    oper.AN_DATEWYP >= ills.AN_DATEWYP,
                    oper.AN_DATEWYP <= ills.I_DATEOU_O)).group_by(oper.AN_CLASS_OPER)

#    print(str(q))
   

    q = q.order_by(func.count(oper.AN_CLASS_OPER).desc())                    


#    [print(f"{i.kartotek.K_NAME:<15}  {i.oper.AN_DATEWYP} {i.oper.AN_NAME_OPER:<50} {i.cnt}") for i in q.all()]

    [print(f"{str(i.oper.AN_NAME_OPER):<50} {i.cnt}") for i in q.all()]




# --------------------------------------------------
if __name__ == '__main__':
    main()

