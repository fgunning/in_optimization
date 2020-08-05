import json, requests, tableauserverclient as TSC, pandas as pd, re
#Tableau Server variables
ts_url = 'https://demo.tableau.com'
ts_site = 'Fearghal'
tableau_auth = TSC.TableauAuth('fgunning', 'password', ts_site)
server = TSC.Server(ts_url)
#query for the metadata API
mdapi_query = '''query calcs {
  calculatedFields {
    name
    formula
    datasource {
        name
      ...on PublishedDatasource {
        luid
        name
        vizportalUrlId
      }
      ...on EmbeddedDatasource {
        workbook {
          luid
          vizportalUrlId
          name
        }
      }
    }
  }
}'''


with server.auth.sign_in(tableau_auth):
    token = server.auth_token
    auth_headers = auth_headers = {'accept': 'application/json','content-type': 'application/json','x-tableau-auth': token}
    metadata_query = requests.post(ts_url + '/api/metadata/graphql', headers = auth_headers, verify=True, json = {"query": mdapi_query})
    mdapi_result = json.loads(metadata_query.text)
    
    #create the dataframe from our MDAPI query result and pad out any NULLs
    calcs_df = pd.DataFrame(mdapi_result['data']['calculatedFields'])
    calcs_df.fillna(value={'datasource': 'embedded', 'formula': 'placeholder'}, inplace=True)

    #create the columns we want in our output file
    calcs_df['most_ORs']=0;
    calcs_df['total_ORs'] = 0;
    calcs_df['type'] = 'placeholder';
    calcs_df['url'] = 'placeholder'

    for index, row in calcs_df.iterrows():
        formula = row['formula'].upper()
        #find every instance of IF or WHEN, indicating the start of a logic string
        or_instances = [m.start() for m in re.finditer('(IF|WHEN)', formula)]
        or_count = []
        for a in or_instances:
            #find the THEN that ends the logic string
            next_then = (formula.find('THEN',a))
            #count the ORs in between
            or_count.append(formula.count(' OR ',a, next_then))
            most_ORs = max(or_count)
            #compute longest string of consecutive ORs within the calc, as well as total ORs within the calc
            calcs_df.at[index, 'most_ORs'] = most_ORs
            calcs_df.at[index, 'total_ORs'] = sum(or_count)
        #determine if datasource is embedded or published and find appropriate URL
        if 'workbook' in calcs_df.at[index, 'datasource']:
            workbook_portal_id = calcs_df.at[index, 'datasource']['workbook']['vizportalUrlId']
            url = ts_url + '/#/site/' + ts_site + '/workbooks/' + workbook_portal_id
            calcs_df.at[index, 'url'] = url
            calcs_df.at[index, 'type'] = 'embedded datasource';
            calcs_df.at[index, 'datasource'] = calcs_df.at[index, 'datasource']['workbook']['name']
        elif 'luid' in calcs_df.at[index, 'datasource']:
            datasource_portal_id = calcs_df.at[index, 'datasource']['vizportalUrlId']
            ds_url = ts_url + '/#/site/' + ts_site + '/datasources/' + datasource_portal_id  + '/AskData'
            calcs_df.at[index, 'url'] = ds_url
            calcs_df.at[index, 'type'] = 'published datasource';
        #eliminate all calcs that don't have any ORs
        if calcs_df.at[index, 'total_ORs'] == 0:
            calcs_df.drop(index, inplace=True);
    #people often put line breaks in their logic statements... remove those so our CSV is cleaner
    calcs_df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
    calcs_df.to_csv('orfile.csv', index_label=True)
