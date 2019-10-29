

#junk from the commonfunction - individual data section
     #final_series = final_series.append(new_series)
            ##convert series to df
            #new_df = new_series.to_frame()

            ##convert series name to column in df
            #new_df['measure'] = new_series.name
            
            ##add new column to existing multilevel index
            #new_df.set_index('measure', append=True,inplace=True)

            #new_df.reset_index(inplace=True)
            #print(new_df)



            #ist_of_df.append(new_df)
            
            #final_df = pd.concat([final_df,new_df],join='outer')

            #print(new_series)
            #list_of_series.append(new_series)

            #print(final_df.info())
            
            #list_of_series.append(pd.Series(new_series))
            
            #
            #df1 = (pd.concat([list_of_df],keys=(key_elements))
            #       .reorder_levels([0,1,2,3,4,5])
            #       .to_frame(list_of_df.name))

            #list_of_df.append(df1)

            #for i in list_of_df:
            #    final_df = list_of_df.append(i)

