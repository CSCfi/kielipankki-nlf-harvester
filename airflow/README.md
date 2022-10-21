## Run a simple download DAG in Airflow locally in Docker

Download METS file for binding 379973 and all ALTO files listed in the downloaded METS.

---

### Requirements

- Docker compose and Docker engine installed

### Instructions

1. Open a terminal and navigate to `airflow/` folder

2. Add user ID to `.env` with the following command:

    ```echo -e "AIRFLOW_UID=$(id -u)" > .env```

3. Initialize Airflow in docker:

    ```docker compose up airflow-init```

4. Build image and run container:

    ```docker compose up --build```

5. Wait for the container to launch, and open Airflow in `http://0.0.0.0:8080/`

6. Login with the created default username `airflow` and password `airflow`

7. Turn on the DAG `download_altos_for_binding` and watch the green dots appear

8. You can trigger the execution of the DAG again by pressing the "play" button on the right side of the DAG UI and selecting "Trigger DAG"

&nbsp;

`./downloads/mets` should now have one METS file and `downloads/[BINDING_ID]/alto` four ALTO files identified from the downloaded METS.
