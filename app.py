from flask import Flask, request, render_template, jsonify
import requests
from io import StringIO
import pandas as pd
import traceback

app = Flask(__name__)

# Load TSV files from Dropbox links
def load_data():
    # Define the data types for the two datasets
    dtypes_results = {
        'personId': 'string',
        'eventId': 'string',
        'personCountryId': 'string',
        'best': 'int32',
        'personName': 'string'
    }
    dtypes_ranks = {
        'personId': 'string',
        'eventId': 'string',
        'countryRank': 'float32',
        'best': 'int32'
    }

    # Dropbox links
    ranks_link = "https://www.dropbox.com/scl/fi/69fuhncnag3nelmvwzxb8/WCA_export_RanksSingle.tsv?rlkey=2t2bnehdbi25a40qyc659jxhv&st=42qv8yqs&dl=1"
    results_link = "https://www.dropbox.com/scl/fi/js90qjcxckuld3gmxi3lg/WCA_export_Results.tsv?rlkey=hdx54ocgglhhlg7bhp47t6ig6&st=6a7qvnwr&dl=1"

    def read_tsv_from_dropbox(link, dtype, usecols):
        """Reads TSV data from a Dropbox link into a DataFrame."""
        try:
            response = requests.get(link)
            response.raise_for_status()
            return pd.read_csv(StringIO(response.text), sep="\t", dtype=dtype, usecols=usecols)
        except requests.exceptions.RequestException as e:
            print(f"Error downloading data: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error

    # Load data
    results_df = read_tsv_from_dropbox(results_link, dtypes_results, ['personId', 'eventId', 'personCountryId', 'best', 'personName'])
    ranks_df = read_tsv_from_dropbox(ranks_link, dtypes_ranks, ['personId', 'eventId', 'countryRank', 'best'])

    return results_df, ranks_df

# Initialize and load the data
results_df, ranks_df = load_data()

# Merge DataFrames on common columns
def merge_data(results_df, ranks_df):
    """Merge results and ranks DataFrames."""
    return pd.merge(
        results_df,
        ranks_df,
        on=["personId", "eventId", "best"],
        how="inner"
    )

# Merge the data
merged_df = merge_data(results_df, ranks_df)

# Helper Functions
def format_rank(rank):
    """Format rank into a readable string."""
    rank = int(rank)
    if rank == 1:
        return f"{rank}st"
    elif rank == 2:
        return f"{rank}nd"
    elif rank == 3:
        return f"{rank}rd"
    else:
        return f"{rank}th"

def format_time(centiseconds):
    """Format time from centiseconds to mm:ss.cc."""
    centiseconds = int(centiseconds)
    minutes, centiseconds = divmod(centiseconds, 6000)
    seconds, fractional = divmod(centiseconds, 100)
    return f"{minutes}:{seconds:02d}.{fractional:02d}" if minutes > 0 else f"{seconds}.{fractional:02d}"

def format_best_result(event_id, best):
    """Format the best result based on the event type."""
    if event_id == "333fm":
        return f"{int(best)}"
    elif event_id == "333mbf":
        best_str = f"{int(best):08d}"
        total_points = int(best_str[:2])
        total_time_seconds = int(best_str[2:-2])
        missed_cubes = int(best_str[-2:])
        solved_cubes = 99 - total_points + missed_cubes
        attempted_cubes = solved_cubes + missed_cubes
        minutes, seconds = divmod(total_time_seconds, 60)
        formatted_time = f"{minutes}:{seconds:02d}"
        return f"{solved_cubes}/{attempted_cubes} {formatted_time}"
    else:
        return format_time(best)

@app.route('/')
def home():
    """Home route to display the event list and regions."""
    event_names = {
        "333": "3x3", "222": "2x2", "444": "4x4", "555": "5x5", "666": "6x6", "777": "7x7",
        "333bf": "3x3 Blindfolded", "333fm": "3x3 Fewest Moves", "333oh": "3x3 One-Handed",
        "clock": "Clock", "minx": "Megaminx", "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
        "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded", "333mbf": "3x3 Multi-Blind",
        "333mbo": "3x3 Multi-Blind Old Style", "magic": "Magic", "mmagic": "Master Magic", "333ft": "3x3 With Feet",
    }
    events = sorted(event_names.keys())
    regions = merged_df['personCountryId'].unique()
    return render_template('index.html', events=events, regions=regions, event_names=event_names)

@app.route('/search', methods=['POST'])
def search():
    """Search route to query the rankings and results based on user input."""
    try:
        event_names = {
            "333": "3x3", "222": "2x2", "444": "4x4", "555": "5x5", "666": "6x6", "777": "7x7",
            "333bf": "3x3 Blindfolded", "333fm": "3x3 Fewest Moves", "333oh": "3x3 One-Handed",
            "clock": "Clock", "minx": "Megaminx", "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
            "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded", "333mbf": "3x3 Multi-Blind",
            "333mbo": "3x3 Multi-Blind Old Style", "magic": "Magic", "mmagic": "Master Magic", "333ft": "3x3 With Feet",
        }

        event_id = request.form.get('event_id')
        region = request.form.get('region')
        rank_number = request.form.get('rank_number')

        if rank_number.lower() == "lowest":
            rank_number = merged_df.query(f"eventId == '{event_id}' and personCountryId == '{region}'")['countryRank'].max()
        else:
            rank_number = int(rank_number)

        filtered_df = merged_df.query(f"eventId == '{event_id}' and personCountryId == '{region}' and countryRank == {rank_number}")
        if not filtered_df.empty:
            person = filtered_df.iloc[0]
            return render_template(
                'result.html',
                personName=person['personName'],
                event=event_id,
                country=person['personCountryId'],
                rank=format_rank(person['countryRank']),
                bestResult=format_best_result(person['eventId'], person['best']),
                event_names=event_names
            )
        else:
            return render_template('error.html', message="No person found with the specified criteria.")
    except Exception as e:
        return render_template('error.html', message=str(e), traceback=traceback.format_exc())

if __name__ == '__main__':
    app.run(debug=True)
