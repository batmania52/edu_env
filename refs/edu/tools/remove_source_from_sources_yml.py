import yaml
import os
import argparse

def remove_source_entry(sources_file_path, source_name, table_name):
    """sources.yml 파일에서 지정된 소스 정의를 삭제합니다."""
    sources_dict = {"version": 2, "sources": []}

    if os.path.exists(sources_file_path):
        with open(sources_file_path, 'r', encoding='utf-8') as f:
            try:
                existing_data = yaml.safe_load(f)
                if existing_data and "sources" in existing_data:
                    sources_dict = existing_data
            except yaml.YAMLError as e:
                print(f"경고: 기존 sources.yml 파일 {sources_file_path} 파싱 오류: {e}. 삭제 작업을 중단합니다.")
                return False

    initial_sources_count = len(sources_dict.get("sources", []))
    
    new_sources = []
    found_and_removed = False
    for source_entry in sources_dict.get("sources", []):
        if source_entry.get("name") == source_name:
            initial_tables_count = len(source_entry.get("tables", []))
            source_entry["tables"] = [table for table in source_entry.get("tables", []) if table.get("name") != table_name]
            if len(source_entry["tables"]) < initial_tables_count:
                print(f"정보: 소스 '{source_name}.{table_name}'을(를) {sources_file_path}에서 삭제했습니다.")
                found_and_removed = True
            if source_entry.get("tables"): # 테이블이 하나라도 남아있으면 소스 엔트리 유지
                new_sources.append(source_entry)
            else: # 해당 소스에 테이블이 없으면 소스 엔트리 자체를 삭제
                print(f"정보: 소스 '{source_name}'에 더 이상 테이블이 없어 소스 엔트리 자체를 삭제합니다.")
        else:
            new_sources.append(source_entry)
            
    sources_dict["sources"] = new_sources

    if found_and_removed or len(new_sources) < initial_sources_count:
        with open(sources_file_path, 'w', encoding='utf-8') as f:
            yaml.dump(sources_dict, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)
        return True
    else:
        print(f"정보: 소스 '{source_name}.{table_name}'을(를) {sources_file_path}에서 찾을 수 없습니다. 삭제 작업을 건너뜁니다.")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="sources.yml 파일에서 소스 정의를 삭제합니다.")
    parser.add_argument("--sources_file_path", required=True, help="sources.yml 파일의 전체 경로")
    parser.add_argument("--source_name", required=True, help="삭제할 소스의 이름 (예: edu)")
    parser.add_argument("--table_name", required=True, help="삭제할 소스 테이블의 이름 (예: test_data)")
    
    args = parser.parse_args()

    try:
        remove_source_entry(args.sources_file_path, args.source_name, args.table_name)
    except Exception as e:
        print(f"오류 발생: {e}")
        exit(1)