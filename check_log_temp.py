import psycopg2
from rich.console import Console
from rich.table import Table

def check_dbt_log():
    console = Console()
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            user="airflow",
            password="airflow",
            dbname="airflow"
        )
        cur = conn.cursor()
        
        # 최근 5개의 로그 조회
        query = """
            SELECT 
                model_name, 
                status, 
                start_time, 
                end_time, 
                rows_affected 
            FROM admin.dbt_log 
            ORDER BY start_time DESC 
            LIMIT 5;
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        table = Table(title="admin.dbt_log 최근 데이터 (Top 5)")
        table.add_column("Model Name", style="cyan")
        table.add_column("Status", style="magenta")
        table.add_column("Start Time", style="green")
        table.add_column("End Time", style="green")
        table.add_column("Rows Affected", justify="right")
        
        for row in rows:
            table.add_row(
                str(row[0]), 
                str(row[1]), 
                str(row[2]), 
                str(row[3]), 
                str(row[4])
            )
            
        console.print(table)
        
        cur.close()
        conn.close()
    except Exception as e:
        console.print(f"[bold red]오류 발생:[/bold red] {e}")

if __name__ == "__main__":
    check_dbt_log()
