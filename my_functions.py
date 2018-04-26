def top_n_values(df, grpby, features):
    r_df = None
    for colnam, n, aggcol, aggfunct, newcol, orls in features:
        grpdf = df.groupby(grpby + [colnam]).agg({aggcol:aggfunct}).reset_index()
        grpdf.rename(columns = {aggcol:newcol}, inplace=True)
        grpdf = grpdf.sort_values(by = newcol, ascending = orls)
        grpdf['rank'] = grpdf.groupby(grpby).cumcount()+1
        grpdf = grpdf.query('rank <= {n_giv}'.format(n_giv = n))
        if grpdf.empty:
            grpdf[newcol] = np.nan
        else:
            grpdf[newcol] = grpdf.apply(lambda x: {x[colnam]:x[newcol]},axis = 1)
            grpdf = grpdf.groupby(grpby)[newcol].apply(list).reset_index()
            grpdf[newcol] = convert_list_dict_todict(df=grpdf, colnames=newcol)
        r_df = grpdf if r_df is None else pd.merge(r_df, grpdf, on = grpby, how = 'outer')
    return r_df

def  formula_eval(df, features):
    for formula in features:
        df.eval(formula, inplace = True)
    return df

def unique_df(df,uniq_feat, keep = 'first'):
    return df[uniq_feat].drop_duplicates(keep = keep)

def multi_index_rename(df,sep = '_', chcase = 'lower'):
    if type(df.columns) == pd.core.indexes.multi.MultiIndex:
        col_comps = len(df.columns.labels)
        level_len = len(df.columns.labels[0])
        colname = []
        for i in xrange(0, level_len):
            name = []
            for j in xrange(0, col_comps):
                col_comp =  str(df.columns.levels[j][df.columns.labels[j][i]])
                if col_comp != '':
                     name = name + [col_comp]
            name = sep.join(name)
            colname = colname + [name]
        if chcase == 'lower':
            colname = [i.lower() for i in colname]
        elif chcase == 'upper':
            colname = [i.upper() for i in colname]
        return colname
    else:
        print 'This is not a multi index columns. You are using the wrong function.'
        return 
def convert_list_dict_todict(df,colnames):
    colnames = [colnames] if type(colnames) == str else colnames
    return df[colnames].applymap(lambda x: dict([(key,d[key]) for d in x for key in d]))

#place holder to add functio to join multiple dfs    
def send_mail(send_from,send_to,cc_lst,subject,text,server,username='',password='',isTls=True):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Cc'] = ','.join(cc_lst)
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    part = MIMEBase('application', "octet-stream")
    part.set_payload(open("rep_coaching_QA_output.xlsx", "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="rep_coaching_QA_output.xlsx"')
    msg.attach(part)

    smtp = smtplib.SMTP(server)
    if isTls:
        smtp.starttls()
    smtp.login(username,password)
    smtp.sendmail(send_from, [send_to] + cc_lst, msg.as_string())
    smtp.quit()
    
    def casech_df(df, colname,ch = 'lower'):
    colname = [colname] if type(colname) == str else colname
    if ch.lower() == 'lower':
        df[colname] = df[colname].applymap(lambda x: x.lower() if pd.notnull(x) else x)
        return df
    elif ch.lower() == 'upper':
        df[colname] = df[colname].applymap(lambda x: x.upper() if pd.notnull(x) else x)
        return df
    else:
        return df
    
def regex_replace_df(df,colname,dict_regex):
    colname = [colname] if type(colname) == str else colname
    for i, j in dict_regex.iteritems():
        df[colname] = df[colname].applymap(lambda x :re.sub(pattern = r'%s'%i,repl = j,string = x) if pd.notnull(x) else x)
    return  df
    
def unicode_clean(df,colname,encoding = 'ascii'):
    colname = [colname] if type(colname) == str else colname
    df[colname] = df[colname].applymap(lambda x: x.encode(encoding, 'ignore').decode("utf-8"))
    return df
