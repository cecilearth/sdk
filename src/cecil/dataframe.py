import pandas as pd
import time

from .models import SubscriptionParquetFiles


def load_dataframe(res: SubscriptionParquetFiles) -> pd.DataFrame:
    if not res.files:
        return pd.DataFrame()

    return pd.concat(
        (
            _retry_with_exponential_backoff(pd.read_parquet, 5, 1, 2, f)
            for f in res.files
        )
    ).reset_index(drop=True)


def _retry_with_exponential_backoff(
    func, retries: int, start_delay: int, multiplier: float, *args, **kwargs
):
    delay = start_delay

    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == retries:
                raise e

            time.sleep(delay)
            delay *= multiplier

    return None
