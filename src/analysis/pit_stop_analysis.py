import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import os
from collections import defaultdict, Counter

def analyze_f1_pit_strategy(db_name="f1db_YR=2024"):
    """
    Детальний аналіз піт-стратегій Формули 1 2024:
    - Фільтрація піт-стопів до 60 секунд
    - Аналіз тільки гонок (не кваліфікацій/практик)
    - Стратегічний аналіз залежно від умов гонки
    - Аналіз впливу погоди, позицій, безпечних автомобілів
    """
    # Підключення до бази даних
    db_path = f"data/{db_name}/database.db"
    conn = sqlite3.connect(db_path)

    # Створення директорії для результатів
    output_dir = f"data/{db_name}"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "enhanced_pit_analysis.txt")

    # Ініціалізація виводу
    output_content = []

    def print_and_log(text="", file_only=False):
        """Допоміжна функція для виводу в консоль та файл"""
        if not file_only:
            print(text)
        output_content.append(text)

    try:
        print_and_log("🏁 ДЕТАЛЬНИЙ АНАЛІЗ ПІТ-СТРАТЕГІЙ F1 2024")
        print_and_log("=" * 80)
        print_and_log(f"Аналіз створено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print_and_log("=" * 80)
        print_and_log()

        # ============================================
        # 1. ЗБІР ОСНОВНИХ ДАНИХ ПІТ-СТОПІВ
        # ============================================
        print_and_log("📊 ЗБІР ДАНИХ ПІТ-СТОПІВ...")
        print_and_log("-" * 40)

        # Отримання всіх піт-стопів тільки з гонок, з фільтрацією до 60 секунд
        main_pit_query = """
        SELECT
            p.session_key,
            p.driver_number,
            p.lap_number,
            p.pit_duration,
            p.date as pit_time,
            d.broadcast_name,
            d.full_name,
            d.team_name,
            d.team_colour,
            m.meeting_name,
            m.date_start as race_date,
            m.location,
            s.date_start as session_start,
            s.date_end as session_end
        FROM pit p
        JOIN drivers d ON p.driver_number = d.driver_number AND p.session_key = d.session_key
        JOIN sessions s ON p.session_key = s.session_key
        JOIN meetings m ON s.meeting_key = m.meeting_key
        WHERE s.session_name = 'Race' 
            AND p.pit_duration > 0 
            AND p.pit_duration <= 60.0
        ORDER BY m.date_start, p.driver_number, p.lap_number
        """

        pit_data = pd.read_sql_query(main_pit_query, conn)
        pit_data['pit_time'] = pd.to_datetime(pit_data['pit_time'], format='ISO8601')
        pit_data['race_date'] = pd.to_datetime(pit_data['race_date'])

        print_and_log(f"Валідних піт-стопів знайдено (≤60с): {len(pit_data)}")
        print_and_log(f"Гран-прі з піт-стопами: {pit_data['meeting_name'].nunique()}")
        print_and_log(f"Команд: {pit_data['team_name'].nunique()}")
        print_and_log(f"Пілотів: {pit_data['broadcast_name'].nunique()}")
        print_and_log()

        # ============================================
        # 2. АНАЛІЗ ПО ГРАН-ПРІ
        # ============================================
        print_and_log("🏆 АНАЛІЗ ПО КОЖНОМУ ГРАН-ПРІ")
        print_and_log("-" * 35)

        for gp_name, gp_data in pit_data.groupby('meeting_name'):
            print_and_log(f"\n📍 {gp_name}")
            print_and_log(f"   Дата: {gp_data['race_date'].iloc[0].strftime('%d.%m.%Y')}")
            print_and_log(f"   Локація: {gp_data['location'].iloc[0]}")
            
            # Основна статистика гонки
            total_stops = len(gp_data)
            avg_duration = gp_data['pit_duration'].mean()
            fastest_stop = gp_data['pit_duration'].min()
            slowest_stop = gp_data['pit_duration'].max()
            teams_count = gp_data['team_name'].nunique()
            
            print_and_log(f"   Піт-стопів: {total_stops}")
            print_and_log(f"   Середній час: {avg_duration:.3f}с")
            print_and_log(f"   Найшвидший: {fastest_stop:.3f}с")
            print_and_log(f"   Найповільніший: {slowest_stop:.3f}с")
            print_and_log(f"   Команд: {teams_count}")

            # Найкращі піт-стопи гонки
            best_stops = gp_data.nsmallest(3, 'pit_duration')
            print_and_log(f"   🏅 Топ-3 піт-стопи:")
            for idx, (_, stop) in enumerate(best_stops.iterrows(), 1):
                print_and_log(f"      {idx}. {stop['broadcast_name']} ({stop['team_name']}) - {stop['pit_duration']:.3f}с (коло {stop['lap_number']})")

        print_and_log()

        # ============================================
        # 3. АНАЛІЗ ВПЛИВУ ПОГОДНИХ УМОВ
        # ============================================
        print_and_log("🌤️ ВПЛИВ ПОГОДНИХ УМОВ НА ПІТ-СТРАТЕГІЮ")
        print_and_log("-" * 45)

        # Отримання детальних погодних даних
        weather_query = """
        SELECT
            w.session_key,
            w.date as weather_time,
            w.air_temperature,
            w.track_temperature,
            w.humidity,
            w.pressure,
            w.rainfall,
            w.wind_speed,
            w.wind_direction,
            m.meeting_name
        FROM weather w
        JOIN sessions s ON w.session_key = s.session_key
        JOIN meetings m ON s.meeting_key = m.meeting_key
        WHERE s.session_name = 'Race'
        ORDER BY w.date
        """
        weather_data = pd.read_sql_query(weather_query, conn)
        weather_data['weather_time'] = pd.to_datetime(weather_data['weather_time'], format='ISO8601')

        # Об'єднання погодних даних з піт-стопами
        pit_weather = pd.merge_asof(
            pit_data.sort_values('pit_time'),
            weather_data.sort_values('weather_time'),
            left_on='pit_time',
            right_on='weather_time',
            by='session_key',
            direction='nearest',
            tolerance=pd.Timedelta('10 minutes')
        )

        # Аналіз дощових умов
        rain_threshold = 1.0
        if pit_weather['rainfall'].notna().any():
            rain_conditions = pit_weather[pit_weather['rainfall'] >= rain_threshold]
            dry_conditions = pit_weather[pit_weather['rainfall'] < rain_threshold]
            
            if not rain_conditions.empty and not dry_conditions.empty:
                print_and_log(f"Піт-стопи під дощем (≥{rain_threshold}мм): {len(rain_conditions)}")
                print_and_log(f"Піт-стопи в суху погоду: {len(dry_conditions)}")
                print_and_log(f"Середній час під дощем: {rain_conditions['pit_duration'].mean():.3f}с")
                print_and_log(f"Середній час в суху погоду: {dry_conditions['pit_duration'].mean():.3f}с")
                print_and_log(f"Різниця: {rain_conditions['pit_duration'].mean() - dry_conditions['pit_duration'].mean():+.3f}с")
                print_and_log()

        # Аналіз температурних умов
        if pit_weather['track_temperature'].notna().any():
            temp_median = pit_weather['track_temperature'].median()
            hot_conditions = pit_weather[pit_weather['track_temperature'] > temp_median]
            cool_conditions = pit_weather[pit_weather['track_temperature'] <= temp_median]
            
            print_and_log(f"Медіанна температура треку: {temp_median:.1f}°C")
            print_and_log(f"Піт-стопи в жарких умовах (>{temp_median:.1f}°C): {len(hot_conditions)}")
            print_and_log(f"Піт-стопи в прохолодних умовах (≤{temp_median:.1f}°C): {len(cool_conditions)}")
            if not hot_conditions.empty and not cool_conditions.empty:
                print_and_log(f"Середній час у жару: {hot_conditions['pit_duration'].mean():.3f}с")
                print_and_log(f"Середній час у прохолоді: {cool_conditions['pit_duration'].mean():.3f}с")
                print_and_log(f"Різниця: {hot_conditions['pit_duration'].mean() - cool_conditions['pit_duration'].mean():+.3f}с")
            print_and_log()

        # ============================================
        # 4. АНАЛІЗ ВПЛИВУ БЕЗПЕЧНИХ АВТОМОБІЛІВ
        # ============================================
        print_and_log("🚩 ВПЛИВ БЕЗПЕЧНИХ АВТОМОБІЛІВ ТА ПРАПОРІВ")
        print_and_log("-" * 45)

        # Отримання повідомлень від директора гонки
        race_control_query = """
        SELECT
            rc.session_key,
            rc.date,
            rc.lap_number,
            rc.driver_number,
            rc.message,
            rc.flag,
            rc.category,
            rc.scope,
            m.meeting_name
        FROM race_control rc
        JOIN sessions s ON rc.session_key = s.session_key
        JOIN meetings m ON s.meeting_key = m.meeting_key
        WHERE s.session_name = 'Race'
            AND (
                LOWER(rc.message) LIKE '%safety car%' OR
                LOWER(rc.message) LIKE '%virtual safety car%' OR
                LOWER(rc.message) LIKE '%vsc%' OR
                LOWER(rc.message) LIKE '%yellow%' OR
                LOWER(rc.message) LIKE '%red flag%'
            )
        ORDER BY s.date_start, rc.date
        """
        race_control_data = pd.read_sql_query(race_control_query, conn)

        # Аналіз піт-стопів під час безпечних автомобілів
        sc_sessions = race_control_data[
            race_control_data['message'].str.contains('SAFETY CAR', case=False, na=False)
        ]['session_key'].unique()

        vsc_sessions = race_control_data[
            race_control_data['message'].str.contains('VIRTUAL SAFETY CAR|VSC', case=False, na=False)
        ]['session_key'].unique()

        if len(sc_sessions) > 0:
            sc_races = pit_data[pit_data['session_key'].isin(sc_sessions)]
            print_and_log(f"Гонки з Safety Car: {sc_races['meeting_name'].nunique()}")
            print_and_log(f"Піт-стопи в SC гонках: {len(sc_races)}")
            print_and_log(f"Середній час піт-стопів в SC гонках: {sc_races['pit_duration'].mean():.3f}с")

        if len(vsc_sessions) > 0:
            vsc_races = pit_data[pit_data['session_key'].isin(vsc_sessions)]
            print_and_log(f"Гонки з VSC: {vsc_races['meeting_name'].nunique()}")
            print_and_log(f"Піт-стопи в VSC гонках: {len(vsc_races)}")
            print_and_log(f"Середній час піт-стопів в VSC гонках: {vsc_races['pit_duration'].mean():.3f}с")

        print_and_log()

        # ============================================
        # 5. АНАЛІЗ СТРАТЕГІЙ КОМАНД
        # ============================================
        print_and_log("🏁 ПОРІВНЯННЯ СТРАТЕГІЙ КОМАНД")
        print_and_log("-" * 35)

        team_analysis = pit_data.groupby('team_name').agg({
            'pit_duration': ['count', 'mean', 'std', 'min', 'max'],
            'lap_number': ['mean', 'std']
        }).round(3)

        team_analysis.columns = ['_'.join(col).strip() for col in team_analysis.columns.values]
        team_analysis = team_analysis.sort_values('pit_duration_mean')

        print_and_log(f"{'Команда':<25} {'Стопи':>6} {'Сер.час':>8} {'Відхил.':>8} {'Кращий':>8} {'Гірший':>8}")
        print_and_log("-" * 75)

        for team in team_analysis.index:
            stops = int(team_analysis.loc[team, 'pit_duration_count'])
            avg_time = team_analysis.loc[team, 'pit_duration_mean']
            std_dev = team_analysis.loc[team, 'pit_duration_std']
            best_time = team_analysis.loc[team, 'pit_duration_min']
            worst_time = team_analysis.loc[team, 'pit_duration_max']

            print_and_log(f"{team[:24]:<25} {stops:>6} {avg_time:>8.3f} {std_dev:>8.3f} {best_time:>8.3f} {worst_time:>8.3f}")

        print_and_log()

        # ============================================
        # 6. АНАЛІЗ ВПЛИВУ ПОЗИЦІЇ НА ПІТ-СТРАТЕГІЮ
        # ============================================
        print_and_log("📍 ВПЛИВ ПОЗИЦІЇ НА ПІТ-СТРАТЕГІЮ")
        print_and_log("-" * 35)

        # Отримання позицій перед піт-стопами
        position_query = """
        SELECT
            p.session_key,
            p.driver_number,
            p.lap_number as pit_lap,
            p.pit_duration,
            d.broadcast_name,
            d.team_name,
            pos.position,
            m.meeting_name
        FROM pit p
        JOIN drivers d ON p.driver_number = d.driver_number AND p.session_key = d.session_key
        JOIN sessions s ON p.session_key = s.session_key
        JOIN meetings m ON s.meeting_key = m.meeting_key
        LEFT JOIN position pos ON p.session_key = pos.session_key 
                               AND p.driver_number = pos.driver_number
        WHERE s.session_name = 'Race' 
            AND p.pit_duration > 0 
            AND p.pit_duration <= 60.0
            AND pos.position IS NOT NULL
        """
        
        position_data = pd.read_sql_query(position_query, conn)
        
        if not position_data.empty:
            # Групування по позиціях
            position_groups = {
                'Топ-3 (1-3)': position_data[position_data['position'] <= 3],
                'Очкова зона (4-10)': position_data[(position_data['position'] >= 4) & (position_data['position'] <= 10)],
                'Без очок (11+)': position_data[position_data['position'] >= 11]
            }
            
            for group_name, group_data in position_groups.items():
                if not group_data.empty:
                    print_and_log(f"{group_name}:")
                    print_and_log(f"  Піт-стопів: {len(group_data)}")
                    print_and_log(f"  Середній час: {group_data['pit_duration'].mean():.3f}с")
                    print_and_log(f"  Найкращий: {group_data['pit_duration'].min():.3f}с")
                    print_and_log(f"  Найгірший: {group_data['pit_duration'].max():.3f}с")
                    print_and_log()

        # ============================================
        # 7. АНАЛІЗ ШИННИХ СТРАТЕГІЙ
        # ============================================
        print_and_log("🏎️ АНАЛІЗ ШИННИХ СТРАТЕГІЙ")
        print_and_log("-" * 30)

        # Отримання даних про стінти та шини
        stint_query = """
        SELECT
            st.session_key,
            st.driver_number,
            st.stint_number,
            st.lap_start,
            st.lap_end,
            st.compound,
            st.tyre_age_at_start,
            (st.lap_end - st.lap_start + 1) as stint_length,
            d.broadcast_name,
            d.team_name,
            m.meeting_name
        FROM stints st
        JOIN drivers d ON st.driver_number = d.driver_number AND st.session_key = d.session_key
        JOIN sessions s ON st.session_key = s.session_key
        JOIN meetings m ON s.meeting_key = m.meeting_key
        WHERE s.session_name = 'Race'
        ORDER BY m.date_start, st.driver_number, st.stint_number
        """
        
        stint_data = pd.read_sql_query(stint_query, conn)
        
        if not stint_data.empty:
            compound_stats = stint_data.groupby('compound').agg({
                'stint_length': ['mean', 'median', 'count'],
                'tyre_age_at_start': ['mean', 'median']
            }).round(2)
            
            compound_emojis = {
                'SOFT': '🔴', 'MEDIUM': '🟡', 'HARD': '⚪',
                'INTERMEDIATE': '🟢', 'WET': '🔵'
            }
            
            print_and_log("Статистика використання шин:")
            for compound in compound_stats.index:
                if pd.notna(compound):
                    emoji = compound_emojis.get(compound, '❓')
                    count = int(compound_stats.loc[compound, ('stint_length', 'count')])
                    avg_length = compound_stats.loc[compound, ('stint_length', 'mean')]
                    avg_age = compound_stats.loc[compound, ('tyre_age_at_start', 'mean')]
                    
                    print_and_log(f"  {emoji} {compound}:")
                    print_and_log(f"    Використано в {count} стінтах")
                    print_and_log(f"    Середня довжина стінту: {avg_length:.1f} кіл")
                    print_and_log(f"    Середній початковий знос: {avg_age:.1f} кіл")
            print_and_log()

        # ============================================
        # 8. АНАЛІЗ ЕФЕКТИВНОСТІ ПІТ-ЛЕЙНІВ
        # ============================================
        print_and_log("⚡ ЕФЕКТИВНІСТЬ ПІТ-ЛЕЙНІВ ПО ТРАСАХ")
        print_and_log("-" * 40)

        track_analysis = pit_data.groupby('meeting_name').agg({
            'pit_duration': ['count', 'mean', 'std', 'min', 'max']
        }).round(3)
        
        track_analysis.columns = ['_'.join(col).strip() for col in track_analysis.columns.values]
        track_analysis = track_analysis.sort_values('pit_duration_mean')
        
        print_and_log("Рейтинг найшвидших піт-лейнів:")
        for i, (track, data) in enumerate(track_analysis.head(10).iterrows(), 1):
            avg_time = data['pit_duration_mean']
            total_stops = int(data['pit_duration_count'])
            best_time = data['pit_duration_min']
            
            print_and_log(f"  {i:2d}. {track}")
            print_and_log(f"      Середній час: {avg_time:.3f}с ({total_stops} стопів)")
            print_and_log(f"      Найкращий час: {best_time:.3f}с")
        print_and_log()

        # ============================================
        # 9. ПІДСУМКИ ТА РЕКОМЕНДАЦІЇ
        # ============================================
        print_and_log("🎯 ПІДСУМКИ ТА СТРАТЕГІЧНІ РЕКОМЕНДАЦІЇ")
        print_and_log("-" * 45)

        # Загальна статистика
        total_stops = len(pit_data)
        overall_avg = pit_data['pit_duration'].mean()
        fastest_overall = pit_data['pit_duration'].min()
        slowest_overall = pit_data['pit_duration'].max()
        consistency = pit_data['pit_duration'].std()

        print_and_log("Загальна статистика сезону:")
        print_and_log(f"  • Загальна кількість піт-стопів: {total_stops}")
        print_and_log(f"  • Середній час піт-стопу: {overall_avg:.3f}с")
        print_and_log(f"  • Найшвидший піт-стоп сезону: {fastest_overall:.3f}с")
        print_and_log(f"  • Найповільніший піт-стоп: {slowest_overall:.3f}с")
        print_and_log(f"  • Стандартне відхилення: {consistency:.3f}с")
        print_and_log()

        # Найкращі команди
        best_team = team_analysis.index[0]
        best_team_avg = team_analysis.loc[best_team, 'pit_duration_mean']
        
        print_and_log("Ключові висновки:")
        print_and_log(f"  • Найефективніша команда: {best_team} ({best_team_avg:.3f}с)")
        
        if not track_analysis.empty:
            fastest_track = track_analysis.index[0]
            fastest_track_avg = track_analysis.loc[fastest_track, 'pit_duration_mean']
            print_and_log(f"  • Найшвидший піт-лейн: {fastest_track} ({fastest_track_avg:.3f}с)")
        
        print_and_log()
        print_and_log("Стратегічні рекомендації:")
        print_and_log("  1. 🌧️  Моніторинг погодних умов критично важливий для оптимізації піт-стратегії")
        print_and_log("  2. 🚗  Використання періодів Safety Car/VSC для стратегічних піт-стопів")
        print_and_log("  3. 🏁  Фокус на стабільності роботи піт-команди для зменшення варіативності")
        print_and_log("  4. 📊  Аналіз конкурентів для виявлення стратегічних можливостей")
        print_and_log("  5. 🔧  Адаптація стратегії залежно від особливостей кожної траси")
        
        print_and_log()
        print_and_log("=" * 80)
        print_and_log("АНАЛІЗ ЗАВЕРШЕНО")
        print_and_log(f"Результати збережено в: {output_file}")
        print_and_log("=" * 80)

        # Збереження в файл
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_content))

        print(f"\n✅ Аналіз завершено! Результати збережено в {output_file}")

    except sqlite3.Error as e:
        error_msg = f"Помилка бази даних: {e}"
        print_and_log(error_msg)
        print(error_msg)
    except Exception as e:
        error_msg = f"Помилка: {e}"
        print_and_log(error_msg)
        print(error_msg)
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_f1_pit_strategy()
