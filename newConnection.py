from databricks.sdk import WorkspaceClient
import requests
import json
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State
import tokens
from datetime import datetime


# get auth tokens
DENV = tokens.DBXEnvironment("dev")

# assign the token and host to variables
DOMAIN = DENV.host
TOKEN = DENV.token

# API endpoints for listing jobs
job_url = DOMAIN + '/api/2.0/jobs/list'
jobRuns_url = DOMAIN + '/api/2.0/jobs/runs/list'

# Prepare the header for authentication
headers = {
    'Authorization': f'Bearer {TOKEN}'
}

# initialize the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# get the runs of a job
def jobRuns(job_id):
  payload = {'job_id': job_id}
  responseR = requests.get(jobRuns_url, headers = headers, params = payload)
  if responseR.status_code == 200:
        return responseR.json().get('runs', [])
  else:
        print(f"Failed to list runs for job {job_id}: {responseR.status_code} - {responseR.text}")
        return []


# get jobs
def list_jobs():
    response = requests.get(job_url, headers=headers)
    if response.status_code == 200:
        # Parse and print the job list
        jobs = response.json().get('jobs', [])
        return jobs
           
    else:
        print(f"Failed to list jobs: {response.status_code} - {response.text}")

# the jobs variable is initialized
jobs = list_jobs()

# Fetches only the last run of a job. Greatly accelerates the initialization
def lastRun(job_id):
    payload = {'job_id': job_id, 'active_only': False, 'limit': 1}  # Fetch only the most recent run
    responseR = requests.get(jobRuns_url, headers=headers, params=payload)
    if responseR.status_code == 200:
        runs = responseR.json().get('runs', [])
        if runs:
            run = runs[0]
            
            start_time = run.get('start_time')
            end_time = run.get('end_time')
            duration_ms = end_time - start_time

            # Format start time
            run['formatted_start_time'] = datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if start_time else 'N/A'
            # Format duration
            run['formatted_duration'] = str(duration_ms // 1000) + ' seconds' if duration_ms else 'N/A'  # Convert ms to seconds
            # Include result state
            run['result_state'] = run.get('state', {}).get('result_state', 'N/A')

            #the duration is displated as 0 unless the job is complete
            if run['result_state'] == 'N/A': run['formatted_duration'] = str(0) 
            
            # aids the next function by assingning a color depending on the result
            result_color = '#00f600' if run['result_state'] == 'SUCCESS' else 'red' if run['result_state'] == 'FAILED' else 'blue' if run['result_state'] == 'RUNNING' else 'black'
            run['result_color'] = result_color

            return run
        else:
            return None


# Function to create rows of cards
def create_card_rows(jobs, cards_per_row=4):
    # Dictionary to hold the last run details for each job
    last_run_details = {job['job_id']: lastRun(job['job_id']) for job in jobs}
    
    # cards are generated in rows
    rows = []
    for i in range(0, len(jobs), cards_per_row):
        row_jobs = jobs[i:i+cards_per_row]
        row = dbc.Row(
            [dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H5(job.get('settings', {}).get('name', f"Job ID: {job['job_id']}"), className="card-title",
                                style={'color': 'black', 'font-size': '30px'}),
                        html.P("Last Run's Details:", style={'color': 'black', 'font-size': '20px'}),
                        html.P(f"Last Run ID: {last_run_details[job['job_id']].get('run_id', 'N/A') if last_run_details[job['job_id']] else 'N/A'}"),
                        html.P(f"Last Run Start Time: {last_run_details[job['job_id']].get('formatted_start_time', 'N/A') if last_run_details[job['job_id']] else 'N/A'}"),
                        html.P(f"Duration: {last_run_details[job['job_id']].get('formatted_duration', 'N/A') if last_run_details[job['job_id']] else 'N/A'}"),
                        html.P(f"State: {last_run_details[job['job_id']].get('state', {}).get('life_cycle_state', 'N/A') if last_run_details[job['job_id']] else 'N/A'}"),
                        html.P(f"Result: {last_run_details[job['job_id']].get('result_state', 'N/A') if last_run_details[job['job_id']] else 'N/A'}", 
                               style= {'color': last_run_details[job['job_id']].get('result_color', 'N/A') if last_run_details[job['job_id']] else 'N/A'}),
                        dbc.Button("Show All Runs", id={'type': 'show-all-runs-button', 'index': idx}, n_clicks=0, className="button-click-effect")
                    ]),
                    style={"width": "18rem", "margin": "15px", 
                           "border": "4px solid",
                           "border-color": last_run_details[job['job_id']].get('result_color', 'N/A') if last_run_details[job['job_id']] else 'N/A'}
                )
            ) for idx, job in enumerate(row_jobs, start=i)],
            className="mb-4"
        )
        rows.append(row)
    return rows

# initialises the section for the list of runs
def initRunSection():
    return "No Jobs Were Selected"

def listOfJobsW(jobs):
    rows = []
    for job in jobs:
        job_name = job.get('settings', {}).get('name', f"Job ID: {job['job_id']}")
        checkbox = dbc.Checkbox(
            id={"type": "dynamic-checkbox", "index": job['job_id']},
            className="form-check-input",
            label=job_name
        )
        row = dbc.Row(
            dbc.Col(html.Div(checkbox, className="form-check"), width=12),
            className="mb-2"  # Add margin for spacing between rows
        )
        rows.append(row)
    return rows


# App layout
app.layout = html.Div([
    html.H1('Job Details and Runs'),
    html.Div(id='job-cards', children=create_card_rows(jobs)),
    dcc.Interval(
        id='interval-component',
        interval= 60*5*1000,  # in milliseconds so 5 minutes
        n_intervals=0
    ),
    dbc.Button("Configure", id= 'configure-button', n_clicks=0, className="buttonC"),
    html.Div(id='button-click-output', children = initRunSection()),
     dbc.Modal(
            [
                dbc.ModalHeader("Jobs to Display", style = {'font-size': '25px'}),
                dbc.ModalBody(children =listOfJobsW(jobs)),
                dbc.ModalFooter(
                    dbc.Button("Save and Close the Window", id="close", className="buttonConfClose")
                ),
            ],
            id="configure-window",
        )
])


def create_run_list(runs, job_name):
    list_rows = []
    for run in runs:
        # Convert start_time to readable format
        start_time = run.get('start_time')
        if start_time:
            start_timeR = datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            start_timeR = 'N/A'

        # Calculate run duration
        end_time = run.get('end_time')  
        duration_ms = end_time - start_time
        duration = str(duration_ms // 1000) + ' seconds' if duration_ms else 'N/A'

        # Determine color for result state
        result_state = run.get('state', {}).get('result_state', 'N/A')
        result_color = 'green' if result_state == 'SUCCESS' else 'red' if result_state == 'FAILED' else 'blue' if result_state == 'RUNNING' else 'black'
        
        # If the job is pending its duration is set to zero
        if result_state == 'N/A': duration = str(0) 

        # Create a row for each run with border and colored result state
        row = dbc.Row([
            dbc.Col(html.P(job_name), style={'border': '1px solid black', 'font-size': '15px', "margin-left": "20px"}, width=1),
            dbc.Col(html.P(str(run.get('run_id', 'N/A'))), style={'border': '1px solid black', 'font-size': '15px'}, width=2),
            dbc.Col(html.P(start_timeR), style={'border': '1px solid black', 'font-size': '15px'}, width=2),
            dbc.Col(html.P(duration), style={'border': '1px solid black', 'font-size': '15px'}, width=1),
            dbc.Col(html.P(run.get('state', {}).get('life_cycle_state', 'N/A')), style={'border': '1px solid black', 'font-size': '15px'}, width=2),
            dbc.Col(html.P(result_state, style={'color': result_color}), style={'border': '1px solid black', 'font-size': '15px'}, width=3),
        ], className="mb-2")
        list_rows.append(row)
    return list_rows


# Callback for handling button click
@app.callback(
    Output('button-click-output', 'children'),
    [Input({'type': 'show-all-runs-button', 'index': dash.ALL}, 'n_clicks'),
     Input('interval-component', 'n_intervals')],
    [State({'type': 'show-all-runs-button', 'index': dash.ALL}, 'n_clicks')],
)

def display_click(button_clicks, n_intervals, button_states):
    ctx = dash.callback_context
    # Determine what triggered the callback
    if ctx.triggered and ctx.triggered[0]['prop_id'].split('.')[0] != 'interval-component':
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        index = json.loads(button_id)['index']
        # Ensure the button was actually clicked
        if button_states[index] > 0:
            job_id = jobs[index]['job_id']
            runs = jobRuns(job_id)
            job_name = jobs[index].get('settings', {}).get('name', f"Job ID: {jobs[index]['job_id']}")


            output_layout = [html.Div([
             html.P(f"Runs for the Job: {job_name}", 
                   style={'color': 'black', 'font-size': '35px' }),
             dbc.Row([
               dbc.Col(html.P(f"Run Name"), style={'border': '1px solid black', 'font-weight': 'bold', 'font-size': '15px', "margin-left": "20px"}, width=1),
               dbc.Col(html.P(f"Run ID"), style={'border': '1px solid black', 'font-weight': 'bold', 'font-size': '15px'}, width=2),
               dbc.Col(html.P(f"Start Time"), style={'border': '1px solid black', 'font-weight': 'bold', 'font-size': '15px'}, width=2),
               dbc.Col(html.P(f"Duration"), style={'border': '1px solid black', 'font-weight': 'bold', 'font-size': '15px'}, width=1),
               dbc.Col(html.P(f"State"), style={'border': '1px solid black', 'font-weight': 'bold', 'font-size': '15px'}, width=2),
               dbc.Col(html.P((f"Result")), style={'border': '1px solid black', 'font-weight': 'bold', 'font-size': '15px'}, width=3)
               ]),
             html.Div(create_run_list(runs, job_name), style={"margin-top": "16px"})
              ])]

            return output_layout
        return dash.no_update
    return dash.no_update
       
    
    
# Callback to update cards every 5 minutes
@app.callback(
    Output('job-cards', 'children'),
    Input('interval-component', 'n_intervals'),
    prevent_initial_call=True
)

# upgrade cards
def update_cards(n):
    jobs = list_jobs()
    return create_card_rows(jobs)

@app.callback(
    Output("configure-window", "is_open"),
    [Input("configure-button", "n_clicks"), Input("close", "n_clicks")],
    [State("configure-window", "is_open")],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

    

if __name__ == '__main__':
    app.run_server(debug=False)