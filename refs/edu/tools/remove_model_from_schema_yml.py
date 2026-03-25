import yaml
import os
import argparse

def remove_model_entry(schema_file_path, model_name):
    """schema.yml 파일에서 지정된 모델 정의를 삭제합니다."""
    schema_dict = {"version": 2, "models": []}

    if os.path.exists(schema_file_path):
        with open(schema_file_path, 'r', encoding='utf-8') as f:
            try:
                existing_data = yaml.safe_load(f)
                if existing_data and "models" in existing_data:
                    schema_dict = existing_data
            except yaml.YAMLError as e:
                print(f"경고: 기존 schema.yml 파일 {schema_file_path} 파싱 오류: {e}. 삭제 작업을 중단합니다.")
                return False

    initial_model_count = len(schema_dict.get("models", []))
    schema_dict["models"] = [model for model in schema_dict.get("models", []) if model.get("name") != model_name]
    
    if len(schema_dict["models"]) < initial_model_count:
        print(f"정보: 모델 '{model_name}'을(를) {schema_file_path}에서 삭제했습니다.")
        with open(schema_file_path, 'w', encoding='utf-8') as f:
            yaml.dump(schema_dict, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)
        return True
    else:
        print(f"정보: 모델 '{model_name}'을(를) {schema_file_path}에서 찾을 수 없습니다. 삭제 작업을 건너뜁니다.")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="schema.yml 파일에서 모델 정의를 삭제합니다.")
    parser.add_argument("--schema_file_path", required=True, help="schema.yml 파일의 전체 경로")
    parser.add_argument("--model_name", required=True, help="삭제할 모델 이름")
    
    args = parser.parse_args()

    try:
        remove_model_entry(args.schema_file_path, args.model_name)
    except Exception as e:
        print(f"오류 발생: {e}")
        exit(1)
