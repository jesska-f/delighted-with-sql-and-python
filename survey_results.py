print('Info: Starting Delighted Import')
try:
    import g2pg    
    import pandas as pd    
    import sys    
    import delighted    
    from string import punctuation    
    import os    
    delighted.api_key = os.getenv('DELIGHTED_API_KEY')
    
    #Survey Results    
    try:        
        responses = delighted.SurveyResponse.all(per_page=100,page=1)        
        df = pd.DataFrame(responses)        
        df2 = pd.DataFrame()        
        df2 = df2.append(df)        
        i = 2        
        while len(df)==100:            
            responses = delighted.SurveyResponse.all(per_page=100,page=i)            
            df = pd.DataFrame(responses)            
            df2 = df2.append(df)            
            i +=1        
        df2.reset_index(inplace=True,drop=True)        
        print('Info: Getting data from Delighted API Successful')    
    except Exception as e:        
        print('Error:',e)        
        print('Error: Getting data from Delighted API unsuccesful')        
        sys.exit(1)
    df3 = pd.json_normalize(df2['person_properties'])    
    result = pd.concat([df2, df3], axis=1).drop('person_properties', axis=1)    
    result.columns = [("".join([i for i in c if i not in punctuation.replace('_','')])).lower().strip().replace(' ','_') for c in result.columns]    
    result.set_index('id', drop=True, inplace = True)    
    try:        
        g2pg.df_to_db(result,'nps_survey_responses',index_name='id')        
        print('Info: NPS Survey Results succesfully updated in DB')    
    except Exception as e:        
        print('Error:',e)        
        print('Error: NPS Results Update in DB unsucessful')        
        sys.exit(1)
    #Person Results    
    try:        
        people = delighted.Person.list(auto_handle_rate_limits=True)        
        df = pd.DataFrame()        
        for person in people.auto_paging_iter():            
            df = df.append(person, ignore_index = True)        
        print('Info: Getting data from Delighted API for Persons Successful')    
    except Exception as e:        
        print('Error:',e)        
        print('Error: Getting data from Delighted API for Persons  unsuccesful')        
        sys.exit(1)
    df.columns = [("".join([i for i in c if i not in punctuation.replace('_','')])).lower().strip().replace(' ','_') for c in df.columns]    
    df.set_index('id', drop=True, inplace = True)    
    try:        
        g2pg.df_to_db(df,'nps_people',index_name='id')        
        print('Info: NPS People succesfully updated in DB')    
    except Exception as e:        
        print('Error:',e)        
        print('Error: NPS People Update in DB unsucessful')        
        sys.exit(1)
    #Unsubscribe Data    
    try:        
        unsubscribes = delighted.Unsubscribe.all(per_page=100,page=1)        
        df4 = pd.DataFrame(unsubscribes)        
        unsubscribes_df = pd.DataFrame()        
        unsubscribes_df = unsubscribes_df.append(df4)        
        i = 2        
        while len(df)==100:            
            unsubscribes = delighted.Unsubscribe.all(per_page=100,page=i)            
            df4 = pd.DataFrame(unsubscribes)            
            unsubscribes_df = unsubscribes_df.append(df4)            
            i +=1        
            unsubscribes_df.reset_index(inplace=True,drop=True)        
            print('Info: Getting unsubscribes data from Delighted API Successful')    
    except Exception as e:        
        print('Error:',e)        
        print('Error: Getting unsubscribes data from Delighted API unsuccesful')        
        sys.exit(1)    
        
    unsubscribes_df.columns = [("".join([i for i in c if i not in punctuation.replace('_','')])).lower().strip().replace(' ','_') for c in unsubscribes_df.columns]    
    unsubscribes_df.set_index('person_id',inplace=True)    
    try:        
        g2pg.df_to_db(unsubscribes_df,'nps_unsubscribes',index_name='person_id')        
        print('Info: Delighted unsubscribes data succesfully updated in DB')    
    except Exception as e:        
        print('Error:',e)        
        print('Error: Delighted unsubscribes data update in DB unsucessful')        
        sys.exit(1)
except Exception as e:    
    print('Error:',e)
    sys.exit(1)
