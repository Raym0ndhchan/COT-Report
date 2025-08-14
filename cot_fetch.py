name: COT weekly fetch

on:
  schedule:
    # 3:31 PM EDT (UTC-4) — roughly Mar–Nov
    - cron: "31 19 * * 5"
    # 3:31 PM EST (UTC-5) — roughly Nov–Mar
    - cron: "31 20 * * 5"
  workflow_dispatch:

permissions:
  contents: write

jobs:
  fetch:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install system tzdata (for time zones)
        run: |
          sudo apt-get update
          sudo DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run fetcher (retry once on failure)
        env:
          OUT_DIR: data
          YEARS_BACK: "0"
        run: |
          set -e
          python cot_fetch.py || (echo "First attempt failed; retrying in 30s..." && sleep 30 && python cot_fetch.py)
        continue-on-error: true

      - name: Detect changes
        id: changes
        run: |
          git status --porcelain
          if [ -z "$(git status --porcelain)" ]; then
            echo "changed=false" >> $GITHUB_OUTPUT
          else
            echo "changed=true" >> $GITHUB_OUTPUT
          fi

      - name: Commit & push results
        if: steps.changes.outputs.changed == 'true'
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add -A
          git commit -m "COT update: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
          git push
