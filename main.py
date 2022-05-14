import psycopg2
from dates import dates




# account_id,           0
# balance_date,         1
# type_accrual,         2
# saldo_n,              3
# accrual,              4
# recalc,               5
# payment,              6
# saldo_k,              7
# code_account          8
def main(city, house_type):
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

        total = []  # Результаты выполнения скрипта расчета для всех записей

    for result in fetched:

        months = []

        account_id = str(result[0][0])
        balance_date = str(result[0][1])

        if balance_date != "2022-03-01 00:00:00+00:00":
            print("continue 63 ", account_id, balance_date)
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

        with open(city + house_type + ".csv", "a") as f:
            for month in months:
                for line in month:
                    f.write(str(line) + ";")
                f.write("\n")

        total.append(months)

    # for date in dates:
    #     duty_sum = 0
    #     for elem in total:
    #         if elem:
    #             for i in elem:
    #                 if date in i[0]:
    #                     duty_sum += i[1]

    #     print(f"{date}", duty_sum)


city = "Артышта, пгт."
house_type = "2"
main(city, house_type)

city = "Калтан, г."
house_type = "1"
main(city, house_type)
city = "Калтан, г."
house_type = "2"
main(city, house_type)

city = "Киселевск, г."
house_type = "1"
main(city, house_type)
city = "Киселевск, г."
house_type = "2"
main(city, house_type)

city = "Краснобродский, пгт."
house_type = "1"
main(city, house_type)
city = "Краснобродский, пгт."
house_type = "2"
main(city, house_type)

city = "Междуреченск, г."
house_type = "1"
main(city, house_type)
city = "Междуреченск, г."
house_type = "2"
main(city, house_type)

city = "Мыски, г."
house_type = "1"
main(city, house_type)
city = "Мыски, г."
house_type = "2"
main(city, house_type)

city = "Новокузнецк, г."
house_type = "1"
main(city, house_type)
city = "Новокузнецк, г."
house_type = "2"
main(city, house_type)

city = "Новокузнецкий район"
house_type = "1"
main(city, house_type)
city = "Новокузнецкий район"
house_type = "2"
main(city, house_type)

city = "Осинники, г."
house_type = "1"
main(city, house_type)
city = "Осинники, г."
house_type = "2"
main(city, house_type)

city = "Прокопьевск, г."
house_type = "1"
main(city, house_type)
city = "Прокопьевск, г."
house_type = "2"
main(city, house_type)

city = "Прокопьевский район"
house_type = "1"
main(city, house_type)
city = "Прокопьевский район"
house_type = "2"
main(city, house_type)

city = "Тайжина, п."
house_type = "1"
main(city, house_type)
city = "Тайжина, п."
house_type = "2"
main(city, house_type)

city = "Таштагол, г."
house_type = "1"
main(city, house_type)
city = "Таштагол, г."
house_type = "2"
main(city, house_type)

city = "Таштагольский р-н"
house_type = "1"
main(city, house_type)
city = "Таштагольский р-н"
house_type = "2"
main(city, house_type)

city = "Шерегеш, пгт."
house_type = "1"
main(city, house_type)
city = "Шерегеш, пгт."
house_type = "2"
main(city, house_type)
