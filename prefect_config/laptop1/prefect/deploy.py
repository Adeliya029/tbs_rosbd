import argparse
from prefect.client.schemas.schedules import IntervalSchedule
from datetime import timedelta

try:
    from flows.historical_scraping import historical_scraping_flow
    HAS_HISTORICAL = True
except ImportError:
    HAS_HISTORICAL = False

try:
    from flows.health_check import health_check_flow
    HAS_HEALTH_CHECK = True
except ImportError:
    HAS_HEALTH_CHECK = False


def deploy_scraping_pool():
    print("Registering flows ke scraping-pool...")

    if HAS_HISTORICAL:
        historical_scraping_flow.deploy(
            name="historical-scraping-deployment",
            work_pool_name="scraping-pool",
        )
        print("  ✓ historical_scraping_flow → scraping-pool (manual trigger)")
    else:
        print("  ⚠ historical_scraping_flow not available, skipping")

    if HAS_HEALTH_CHECK:
        health_check_flow.deploy(
            name="health-check-deployment",
            work_pool_name="scraping-pool",
            schedule=IntervalSchedule(interval=timedelta(minutes=15)),
        )
        print("  ✓ health_check_flow → scraping-pool (setiap 15 menit)")
    else:
        print("  ⚠ health_check_flow not available, skipping")

    print("scraping-pool deployment selesai.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pool",
        choices=["scraping-pool", "training-pool"],
        required=True,
        help="Pool yang akan di-deploy"
    )
    args = parser.parse_args()

    if args.pool == "scraping-pool":
        deploy_scraping_pool()
