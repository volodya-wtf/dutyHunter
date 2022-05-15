import psycopg2

from dates import dates
from cities import cities


def dutyFind(city, house_type, last_month_pattern):
    ###
    print(f"Город: {city}")
    print(f"Тип:  {house_type}")
    print("")
    print(f"Первый месяц: {last_month_pattern}")
    print("")

    ###
    conn = psycopg2.connect(
        "dbname=population user=postgres password=postgres host=pellegrini.oort.internal"
    )
    cur = conn.cursor()

    raw_select = f"""SELECT DISTINCT account_id 
                    FROM public.balance b JOIN public.personal_account p_a 
                    ON b.account_id = p_a.id 
                    AND p_a.city='{city}' 
                    AND p_a.house_type='{house_type}';"""

    cur.execute(raw_select)

    fetched = cur.fetchall()
    id_s = [f[0] for f in fetched]

    # Получить все записи по требуемым id
    all_items = []
    for id in id_s:
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
        fetched = cur.fetchall()
        all_items.append(fetched)

    conn.close()

    ###
    print(f"Количество id получено: {len(all_items)}")
    print("")
    ###

    total = []  # Результаты выполнения скрипта расчета для всех записей
    # Внешний цикл
    for result in all_items:

        months = []

        account_id = str(result[0][0])
        balance_date = str(result[0][1])

        # Пропуск лицевого, если отсутствует последний месяц
        if balance_date != last_month_pattern:
            continue

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

        # Внутренний цикл
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

            if new_sum_accrual <= 0:
                continue

            if new_saldo_k <= new_sum_accrual:
                duty = new_saldo_k
            else:
                duty = new_sum_accrual

            if duty <= 0:
                if new_saldo_k > duty:
                    months.append(
                        [
                            balance_date,
                            new_saldo_k + new_sum_accrual,
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
        # Запись логов расчета
        with open(city + str(house_type) + "_log" + ".csv", "a") as f:
            for month in months:
                for line in month:
                    f.write(str(line) + ";")
                f.write("\n")

        total.append(months)

    # Запись duty
    for date in dates:
        duty_sum = 0
        for elem in total:
            if elem:
                for i in elem:
                    if date in i[0]:
                        duty_sum += i[1]
        with open(city + str(house_type) + "_duty" + ".csv", "a") as f:
            f.write(f"{date};{duty_sum}\n")

    ###
    print(f"Рассчет для {city} тип {house_type} выполнен")
    print("-" * 80)
    ###


def main():
    last_month_pattern = "2022-03-01 00:00:00+00:00"
    for city in cities:
        for house_type in range(1, 3):
            dutyFind(city, house_type, last_month_pattern)


if __name__ == "__main__":
    main()
