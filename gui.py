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

  """
  Get the runs of a job from the API.
    
  Args:
  job_id (str): The ID of the job for which runs are to be fetched.

  Returns:
  list: A list of runs for the given job, or an empty list if the request fails.
  """ 
  
  payload = {'job_id': job_id}
  responseR = requests.get(jobRuns_url, headers = headers, params = payload)
  if responseR.status_code == 200:
        return responseR.json().get('runs', [])
  else:
        print(f"Failed to list runs for job {job_id}: {responseR.status_code} - {responseR.text}")
        return []



# get jobs
def list_jobs():

    """
    Fetch the list of jobs from the API.
    
    Returns:
    list: A list of jobs, or None if the request fails.
    """
      
    response = requests.get(job_url, headers=headers)
    if response.status_code == 200:
        # Parse the job list
        jobs = response.json().get('jobs', [])
        return jobs
           
    else:
        print(f"Failed to list jobs: {response.status_code} - {response.text}")


def calc_running_job_dur(start_time):

    """
    Calculate the duration of a running job based on its start time.

    Args:
    start_time (int): The start time of the job in epoch milliseconds.

    Returns:
    int: The duration of the job in whole seconds, or None if start_time is not provided.
    """

    if start_time:
        # Convert start_time from epoch milliseconds to a datetime object
        start_time = datetime.fromtimestamp(start_time / 1000)

        # Get the current time
        current_time = datetime.now()

        # Calculate the duration
        duration = current_time - start_time

        # Duration in seconds
        duration_in_seconds = int(duration.total_seconds())

        return duration_in_seconds  
    else:
        return None

# Fetches only the last run of a job. Greatly accelerates the initialization
def lastRun(job_id):

    """
    Fetches the most recent run of a specified job.

    Args:
    job_id (str): The ID of the job for which the last run is to be fetched.

    Returns:
    dict: A dictionary containing details of the last run, or None if no runs are found.
    """

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
            # Include life cycle state
            run['lifecycle_state'] = run.get('state', {}).get('life_cycle_state', 'N/A')

            # The duration is displayed as 0 if the job is pending and if the job is running elapsed time at the moment of refresh is shown
            if run['result_state'] == 'N/A' and not run['lifecycle_state'] == 'RUNNING': run['formatted_duration'] = str(0) + ' seconds'
            if run['lifecycle_state'] == 'RUNNING': run['formatted_duration'] = str(calc_running_job_dur(start_time)) + ' seconds'
            
            # aids the next function by assingning a color depending on the result
            result_color = '#008f00' if run['result_state'] == 'SUCCESS' else 'red' if run['result_state'] == 'FAILED' else '#1e90ff' if run['lifecycle_state'] == 'RUNNING' else 'red' if run['result_state'] == "MAXIMUM_CONCURRENT_RUNS_REACHED" else 'gray'
            run['result_color'] = result_color

            # Depending on the specific lifecycle and result states, determines which of the attribures will be displayed in bold
            if run['lifecycle_state'] == 'RUNNING' or run['lifecycle_state'] == 'PENDING':
                run['lifecycleFont'] = 'bold'
                run['resultFont'] = 'normal'
            else:
                run['lifecycleFont'] = 'normal'
                run['resultFont'] = 'bold'

            
            return run
        else:
            return None


# Function to create rows of cards
def create_card_rows(jobs, cards_per_row=99):

    """
    Creates rows of cards displaying job details.

    Args:
    jobs (list): A list of job dictionaries.
    cards_per_row (int): Number of cards to display per row.

    Returns:
    list: A list of dbc.Row objects, each containing a row of cards.
    """


    # Dictionary to hold the last run details for each job
    last_run_details = {job['job_id']: lastRun(job['job_id']) for job in jobs}
    # cards are generated in rows
    rows = []
    for i in range(0, len(jobs), cards_per_row):
        row_jobs = jobs[i:i+cards_per_row]
        row = dbc.Row(
            [dbc.Card(
                    dbc.CardBody([
                        html.H5(job.get('settings', {}).get('name', f"Job ID: {job['job_id']}"), className="card-title",
                                style={'color': last_run_details[job['job_id']].get('result_color', 'N/A') if last_run_details[job['job_id']] else 'black',
                                      'font-size': '25px'}),       
                        
                        html.P(f"State: {last_run_details[job['job_id']].get('lifecycle_state', 'N/A') if last_run_details[job['job_id']] else 'N/A'}", 
                               style= {'color': last_run_details[job['job_id']].get('result_color', 'N/A') if last_run_details[job['job_id']] else 'black',
                                        'font-size': '16px',
                                        'font-weight': last_run_details[job['job_id']].get('lifecycleFont', 'N/A') if last_run_details[job['job_id']] else 'normal'}),
                        
                        html.P(f"Result: {last_run_details[job['job_id']].get('result_state', 'N/A') if last_run_details[job['job_id']] else 'N/A'}", 
                               style= {'color': last_run_details[job['job_id']].get('result_color', 'N/A') if last_run_details[job['job_id']] else 'black',
                                        'font-size': '16px',
                                        'font-weight': last_run_details[job['job_id']].get('resultFont', 'N/A') if last_run_details[job['job_id']] else 'bold'}),

                        html.P(f"Last Run Start Time: {last_run_details[job['job_id']].get('formatted_start_time', 'N/A') if last_run_details[job['job_id']] else 'N/A'}", style={'font-size': '11px'}),
                        html.P(f"Duration: {last_run_details[job['job_id']].get('formatted_duration', 'N/A') if last_run_details[job['job_id']] else 'N/A'}", style={'font-size': '11px'}),
                        dbc.Button("Show All Runs", id={'type': 'show-all-runs-button', 'index': idx}, n_clicks=0, className="button-click-effect")

                    ]),
                    style={"width": "15rem", 
                           "height": "18rem",
                           "margin-right": "3rem",
                           "margin-left": "1.2rem",
                           "margin-bottom": "1rem",
                           "border": "3px solid",
                           "border-color": last_run_details[job['job_id']].get('result_color', 'N/A') if last_run_details[job['job_id']] else 'black',
                           "backgroundColor" : "#f0f0f0",
                           "position" : "relative"}
                ) for idx, job in enumerate(row_jobs, start=i)]
            ) 
        rows.append(row)
    return rows

# initialises the section for the list of runs
def initRunSection():
    return html.P(f"No Jobs Were Selected", style= {"font-size": "15px", 'margin-top': "4px", 'margin-left': "10px"})


# creates the list with checkboxes on the pop-up window 
def listOfJobsW(jobs, filter_text=""):

    """
    Creates a list with checkboxes for each job.

    Args:
    jobs (list): A list of job dictionaries.
    filter_text (str): Text to filter jobs by name.

    Returns:
    list: A list of dbc.Row objects, each containing a checkbox for a job.
    """

    filtered_jobs = [job for job in jobs if filter_text.lower() in job.get('settings', {}).get('name', f"Job ID: {job['job_id']}").lower()]
    rows = []
    for job in filtered_jobs:
        job_name = job.get('settings', {}).get('name', f"Job ID: {job['job_id']}")
        checkbox = dbc.Checkbox(
            id={"type": "dynamic-checkbox", "index": job['job_id']},
            label=job_name,
            persistence=True,
            value = False,
            className = "custom-checkbox"
        )
        row = dbc.Row(
            dbc.Col(html.P(checkbox, className = "form-check")),
            className="mb-2",  # Add margin for spacing between rows
            style = {"margin-top": "10px"}
        )
        rows.append(row)
    return rows


# App layout
app.layout = html.Div([
    html.H1(html.Span('Job Details and Runs', style={'margin-left': '10px'}), 
            style = {"color" : "#f0f0f0", "backgroundColor" : "#2b2b2b", "padding-bottom": "11px"}),
    html.Div(id='job-cards'),
    dcc.Interval(
        id='interval-component',
        interval= 9999999999999,
        n_intervals=0
    ),
    dbc.Button(html.Img(src="/assets/configure.png", style={'height':'30px', 'width':'30px'}), id= 'configure-button', n_clicks=0, className="buttonC"),
    dbc.Button(html.Img(src="/assets/refresh.png", style={'height':'30px', 'width':'30px'}), id= 'refresh-button', n_clicks=0, className="buttonR"),
    html.Div(id='button-click-output', children = initRunSection()),
    dcc.Store(id='checkbox-states', storage_type='local'),
    dcc.Store(id = 'jobs', storage_type='local', data = list_jobs()),
    dcc.Store(id = 'selected_jobs', storage_type='local'),
    dbc.Modal(
            [
                dbc.ModalHeader(html.H4("Jobs to Display", style={'font-size': '25px', 'color': '#f0f0f0'}),
                   style={'backgroundColor': '#5c5c5c'}),
                dbc.ModalBody([
                   dbc.Input(id="job-search-bar", placeholder="Search jobs...", type="text"),
                   dbc.Col(html.Div(id="job-list-container"))
                 ], style = {"backgroundColor" : "#f0f0f0"}
                ),
                dbc.ModalFooter(
                    dbc.Button("Apply", id="close", className="buttonConfClose"),
                    style = {"backgroundColor" : "#f0f0f0"}
                ),
            ],
            id="configure-window",
        )
    ], style = {"backgroundColor" : "#f0f0f0"}
)
# creates the list that appears after the user clicks a show all runs button
def create_run_list(runs, job_name):

    """
    Creates a list of job runs for display.

    Args:
    runs (list): A list of runs to display.
    job_name (str): The name of the job associated with the runs.

    Returns:
    list: A list of dbc.Row objects, each representing a job run.
    """

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
        lifecycle_state = run.get('state', {}).get('life_cycle_state', 'N/A')
        result_color = '#008f00' if result_state == 'SUCCESS' else 'red' if result_state == 'FAILED' else 'red' if result_state == 'MAXIMUM_CONCURRENT_RUNS_REACHED' else 'black'
        lifecycle_color = '#1e90ff' if lifecycle_state == 'RUNNING' else 'black'

        
        # The duration is displayed as 0 if the job is pending and if the job is running elapsed time at the moment of refresh is shown
        if result_state == 'N/A' and not lifecycle_state == 'RUNNING': duration = str(0) + ' seconds'
        if lifecycle_state == 'RUNNING': duration = str(calc_running_job_dur(start_time)) + ' seconds'


        # Create a row for each run with border and colored result state
        row = dbc.Row([
            dbc.Col(html.P(job_name), style={'border': '1px solid black', 'font-size': '15px', "margin-left": "20px"}, width=1),
            dbc.Col(html.P(str(run.get('run_id', 'N/A'))), style={'border': '1px solid black', 'font-size': '15px'}, width=2),
            dbc.Col(html.P(start_timeR), style={'border': '1px solid black', 'font-size': '15px'}, width=2),
            dbc.Col(html.P(duration), style={'border': '1px solid black', 'font-size': '15px'}, width=1),
            dbc.Col(html.P(lifecycle_state), style={'border': '1px solid black', 'font-size': '15px', 'color': lifecycle_color}, width=2),
            dbc.Col(html.P(result_state, style={'color': result_color}), style={'border': '1px solid black', 'font-size': '15px'}, width=3),
        ])
        list_rows.append(row)
    return list_rows


# Callback for handling button click
@app.callback(
    Output('button-click-output', 'children'),
    [Input({'type': 'show-all-runs-button', 'index': dash.ALL}, 'n_clicks'),
     Input('interval-component', 'n_intervals')],
    [State({'type': 'show-all-runs-button', 'index': dash.ALL}, 'n_clicks'),
     State('selected_jobs', 'data')]
)

def display_click(button_clicks, n_intervals, button_states, selected_jobs):
    ctx = dash.callback_context
    # Determine what triggered the callback
    if ctx.triggered and ctx.triggered[0]['prop_id'].split('.')[0] != 'interval-component':
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        index = json.loads(button_id)['index']
        # Ensure the button was actually clicked
        if button_states[index] > 0:
            job_id = selected_jobs[index]['job_id']
            runs = jobRuns(job_id)
            job_name = selected_jobs[index].get('settings', {}).get('name', f"Job ID: {selected_jobs[index]['job_id']}")

            output_layout = [html.Div([
             html.P(f"Runs for the Job: {job_name}", 
                   style={'color': 'black', 'font-size': '35px', 'margin-left': "10px"}),
             dbc.Row([
               dbc.Col(html.P(f"Job Name"), style={'border': '1px solid black', 'font-weight': 'bold', 'font-size': '15px', "margin-left": "20px"}, width=1),
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
       
    
    
# Callback to update cards when the refresh button is clicked or each time the selection is changed
@app.callback(
    [Output('job-cards', 'children'),
    Output('selected_jobs', 'data'),
    Output('jobs','data')],
    [Input('interval-component', 'n_intervals'),
    Input('checkbox-states', 'data'),
    Input("refresh-button", "n_clicks")],
    State('jobs', 'data'),
)

# upgrade cards
def update_cards(n, checkbox_states, refreshB, jobs):

    if refreshB:
       jobs = list_jobs() #jobs are refetched from databricks every 5 minutes or when the refresh is pressed


     #Filter the jobs based on the checkbox states
    if checkbox_states is None: # if the list is not initialized display all
          selected_jobss = jobs
    else:
          selected_jobss = [job for job in jobs if checkbox_states.get(str(job['job_id']))]
          if selected_jobss == []: #if none are selected display all
              selected_jobss = jobs
    return create_card_rows(selected_jobss), selected_jobss, jobs



#window opener
@app.callback(
    Output("configure-window", "is_open"),
    [Input("configure-button", "n_clicks"), Input("close", "n_clicks")],
    [State("configure-window", "is_open")],
)


def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

#updates the storage that holds which jobs were selected on the pop-up window. Only called after the button on the window is clicked
@app.callback(
    Output('checkbox-states', 'data'),
    Input("close", "n_clicks"),
    [State('jobs', 'data'),
     State({'type': 'dynamic-checkbox', 'index': dash.ALL}, 'value'),
     State('checkbox-states', 'data')],
    prevent_initial_call=True
)

def update_checkbox_states(close_clicks, jobs, checked_states, stored_data):
    if close_clicks:
        updated_data = {}
        for idx, checked in enumerate(checked_states):
            if idx < len(jobs):
             job_id = jobs[idx]['job_id']
             updated_data[str(job_id)] = checked
        return updated_data
    else:
        return dash.no_update
    

# Search function. Also upgrades the list on the pop-up window each time jobs are refreshed. 
@app.callback(
    Output("job-list-container", "children"),
    [Input("job-search-bar", "value"),
    Input('jobs', 'data')]
     
)
def update_job_list(search_value, jobs):
    if not search_value:
        # If the search bar is empty, show all jobs
        return listOfJobsW(jobs)
    else:
        # Filter the jobs based on the search input
        return listOfJobsW(jobs, filter_text=search_value)

if __name__ == '__main__':
    app.run_server(debug=False)