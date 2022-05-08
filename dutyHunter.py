import psycopg2
from dates import dates

conn = psycopg2.connect(
    "dbname=population user=postgres password=postgres host=localhost"
)
cur = conn.cursor()

# Вычитываем все account_id, удовлетворяющие условию
city = "Краснобродский, пгт."
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

    account_id = result[0][0]   # <class 'int'>
    balance_date = str(result[0][1])
    accrual = result[0][4]
    recalc = result[0][5]
    saldo_k = result[0][7]

    if not accrual:
        accrual = 0
    if not recalc:
        recalc = 0
    if not saldo_k:
        saldo_k = 0

    accrual = float(accrual)
    recalc = float(recalc)
    saldo_k = float(saldo_k)

    sum_accrual = accrual + recalc
    if sum_accrual < 0:
        sum_accrual = 0

    # Определение duty
    if saldo_k < sum_accrual:
        duty = saldo_k
    else:
        duty = sum_accrual

    months.append(
        [
            balance_date,
            duty,
            saldo_k,
            sum_accrual,
            account_id,
        ]
    )

    # Для передачи во внутренний цикл
    new_saldo_k = saldo_k
    index_last_elem_on_result = len(result) - 1

    for i in range(1, len(result)):
        balance_date = str(result[i][1])

        # Начисления предыдущего месяц
        accrual = result[i - 1][4]
        recalc = result[i - 1][5]
        if not accrual:
            accrual = 0
        if not recalc:
            recalc = 0
        accrual = float(accrual)
        recalc = float(recalc)

        sum_accrual = accrual + recalc
        if sum_accrual < 0:
            sum_accrual = 0

        ## Новое конечное сальдо текущего месяца равно
        ## сальдо конечное прошлого месяца минус
        ## сумма начислений прошлого месяца
        new_saldo_k = new_saldo_k - sum_accrual

        # Начисления текущего месяца
        new_accrual = result[i][4]
        new_recalc = result[i][5]
        if not new_accrual:
            new_accrual = 0
        if not new_recalc:
            new_recalc = 0
        new_accrual = float(new_accrual)
        new_recalc = float(new_recalc)

        new_sum_accrual = new_accrual + new_recalc
        if new_sum_accrual < 0:
            new_sum_accrual = 0

        ## Выполняем для последнего элемента цикла
        ## Если new_saldo_k >= new_accrual
        if i == index_last_elem_on_result:
            if new_saldo_k >= new_sum_accrual:
                months.append(
                    [
                        balance_date,
                        new_saldo_k,
                        new_saldo_k,
                        new_sum_accrual,
                        account_id,
                    ]
                )
                break


        # При отстутствии начислений в текущем месяце
        if new_accrual <= 0:
            if account_id==876036:
                print("new_accrual <= 0",balance_date, new_saldo_k)
            continue
         

        if account_id==876036:
            print(">",balance_date, new_saldo_k, i, "/", len(result))

        # Определение duty
        if new_saldo_k <= new_sum_accrual:
            duty = new_saldo_k
        else:
            duty = new_sum_accrual

        ## Запись duty
        ## Досрочное завершение цикла
        if duty <= 0:
            if new_saldo_k > duty:
                months.append(
                    [
                        balance_date,
                        new_saldo_k+new_sum_accrual,
                        new_saldo_k,
                        new_sum_accrual,
                        result[i][0],
                    ]
                )
            break
        else:
            months.append(
                [
                    balance_date,
                    duty,
                    new_saldo_k,
                    new_sum_accrual,
                    account_id,
                ]
            )

    # Производим дозапись в журнал отладки
    with open(city + house_type + ".csv", "a") as f:
        for month in months:
            for line in month:
                f.write(str(line) + ";")
            f.write("\n")

    # Записываем результаты вычислени
    total.append(months)


# Суммируем и выводим duty по месяцам
for date in dates:
    duty_sum = 0
    for elem in total:
        if elem:
            for i in elem:
                if date in i[0]:
                    duty_sum += i[1]

    print(f"{date}", duty_sum)
