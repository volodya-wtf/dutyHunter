import psycopg2


conn = psycopg2.connect(
    "dbname=population user=postgres password=postgres host=localhost"
)
cur = conn.cursor()

# Вычитываем все account_id, удовлетворяющие условию

city = "Калтан, г."
house_type = "1"
raw_select = f"""SELECT DISTINCT account_id 
                FROM public.balance b JOIN public.personal_account p_a 
                ON b.account_id = p_a.id 
                AND p_a.city='{city}' 
                AND p_a.house_type='{house_type}';"""

cur.execute(raw_select)

id_s = cur.fetchall()


# Получить все записи по требуемым id
fetched = []
for i in id_s:
    id = str(i[0])
    cur.execute(
        f"""SELECT  account_id, 
                    balance_date, 
                    type_accrual, 
                    saldo_n, 
                    accrual, 
                    recalc, 
                    payment, 
                    saldo_k, 
                    code_account 
            FROM public.balance 
            WHERE account_id={id};"""
    )
    e = cur.fetchall()
    fetched.append(e)


# account_id,           0
# balance_date,         1
# type_accrual,         2
# saldo_n,              3
# accrual,              4
# recalc,               5
# payment,              6
# saldo_k,              7
# code_account          8

total = []  # Результаты выполнения скрипта расчета для всех записей

for result in fetched:

    months = [] 

    n_accrual = result[0][4]    # accrual
    n_recalc = result[0][5]     # recalc
    saldo_k = result[0][7]      # saldo_k
    if not n_accrual:           
        n_accrual = 0
    if not n_recalc:
        n_recalc = 0
    if not saldo_k:
        saldo_k = 0

    sum_n_accrual = n_accrual + n_recalc    # accrual + recalc
    if sum_n_accrual < 0:                   
        sum_n_accrual = 0
    new_saldo_k = saldo_k                   # saldo_k

    if new_saldo_k < sum_n_accrual:         
        duty = new_saldo_k
    else:
        duty = sum_n_accrual

    if duty != 0:
        months.append(
            [
                str(result[0][1]),
                float(duty),
                new_saldo_k,
                n_accrual + n_recalc,
                result[0][0],
            ]
        )

    for i in range(1, len(result)):
        accrual = result[i - 1][4]
        recalc = result[i - 1][5]
        if not accrual:
            accrual = 0
        if not recalc:
            recalc = 0
        if not saldo_k:
            saldo_k = 0

        null_accrual = accrual + recalc
        if null_accrual < 0:
            null_accrual = 0

        new_saldo_k = new_saldo_k - null_accrual
        new_accrual = result[i][4]
        new_recalc = result[i][5]
        if not new_accrual:
            new_accrual = 0
        if not new_recalc:
            new_recalc = 0

        # update
        if new_accrual <= 0:
            #new_saldo_k = 0
            print(str(result[i][1]), result[i][0])
            continue

        sup_accrual = new_accrual + new_recalc
        if sup_accrual < 0:
            sup_accrual = 0

        if new_saldo_k <= sup_accrual:
            duty = new_saldo_k
        else:
            duty = sup_accrual

        # Условие выхода из цикла
        if duty <= 0:
            #if new_saldo_k > duty:
            months.append(
                [
                    str(result[i][1]),
                    float(new_saldo_k+sup_accrual),
                    new_saldo_k,
                    sup_accrual,
                    result[i][0],
                ]
            )
            break
        else:
            months.append(
                [
                    str(result[i][1]),
                    float(duty),
                    new_saldo_k,
                    sup_accrual,
                    result[i][0],
                ]
            )

        saldo_k = new_saldo_k



    with open(city + "_" + house_type + ".csv", "a") as f:
        for month in months:
            for line in month:
                f.write(str(line) + ";")
            f.write("\n")
    total.append(months)

# Debug
# for item in total:
#     for i in item:
# print(i)

dates = [
    "2022-01-01",
    "2022-02-01",
    "2022-03-01",
    "2021-01-01",
    "2021-02-01",
    "2021-03-01",
    "2021-04-01",
    "2021-05-01",
    "2021-06-01",
    "2021-07-01",
    "2021-08-01",
    "2021-09-01",
    "2021-10-01",
    "2021-11-01",
    "2021-12-01",
    "2020-01-01",
    "2020-02-01",
    "2020-03-01",
    "2020-04-01",
    "2020-05-01",
    "2020-06-01",
    "2020-07-01",
    "2020-08-01",
    "2020-09-01",
    "2020-10-01",
    "2020-11-01",
    "2020-12-01",
    "2019-01-01",
    "2019-02-01",
    "2019-03-01",
    "2019-04-01",
    "2019-05-01",
    "2019-06-01",
    "2019-07-01",
    "2019-08-01",
    "2019-09-01",
    "2019-10-01",
    "2019-11-01",
    "2019-12-01",
    "2018-01-01",
    "2018-02-01",
    "2018-03-01",
    "2018-04-01",
    "2018-05-01",
    "2018-06-01",
    "2018-07-01",
    "2018-08-01",
    "2018-09-01",
    "2018-10-01",
    "2018-11-01",
    "2018-12-01",
]

# Суммируем и выводим duty по месяцам
#print(city, house_type)
for date in dates:
    duty_sum = 0

    for elem in total:
        if elem:
            for i in elem:
                if date in i[0]:
                    duty_sum += i[1]

    print(f"{date}", duty_sum)
