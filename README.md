# python-batch

---

## 1. 개요
파이썬 배치 프로젝트 입니다.

## 2. 환경설정
- python 3.9
- 기타 패키지 라이브러리 requirements.txt 참조
  - `pip install -r requierments.txt` 를 통해 한번에 설치 가능.
  - `pip freeze > requirements.txt` 로 패키지 변경 후 버전관리 필요.
- `guide/` 디렉터리에 있는 각 properties를 개발환경에 맞춰 `src/config/` 디렉터리 아래 생성 후 배치 실행.

## 3. 프로젝트 구조
```commandline
python-batch
├─.gitignore
├─README.md
├─requirements.txt
├─guide
│  ├─db_properties.txt
└─src
  ├─config 
  ├─data 
  ├─etl 
  ├─lib 
  │  ├─DBManager.py 
  │  └─LogManager.py
  └─logs
```

## 4. INFO
