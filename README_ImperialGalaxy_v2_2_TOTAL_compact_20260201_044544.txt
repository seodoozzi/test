이 응답에서 '오류로 인해 이 메시지를 표시할 수 없다'가 뜨는 이유(대부분):
- 코드가 너무 길어서 ChatGPT UI가 렌더링을 실패하는 경우가 많습니다.
해결:
- 긴 코드는 채팅에 붙여 넣지 않고 파일로 제공합니다.

파일:
- imperialgalaxy_v2_2_total_compact.py (전체본)

실행:
python imperialgalaxy_v2_2_total_compact.py
python imperialgalaxy_v2_2_total_compact.py --once
python imperialgalaxy_v2_2_total_compact.py --dry-run

로그/DB:
- bot.log
- ig.sqlite
- *.sqlite_copy.json (SQLite에서 자동 생성되는 JSON 스냅샷; 기존 JSON 덮어쓰기 X)
