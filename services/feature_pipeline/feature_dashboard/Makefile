run-frontend-live:
	source ../../../credentials.sh && \
	LIVE=live \
	poetry run streamlit run src/frontend.py

run-frontend-historical:
	source ../../../credentials.sh && \
	LIVE=historical \
	poetry run streamlit run src/frontend.py

run-backend-live:
	source ../../../credentials.sh && \
	LIVE=live \
	poetry run python src/backend.py

run-backend-historical:
	source ../../../credentials.sh && \
	LIVE=historical \
	poetry run python src/backend.py
	