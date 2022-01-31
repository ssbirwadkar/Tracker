import numpy as np

#Get maximum score data frame
def maxcount(df):
    m1 = df.loc[df.groupby(['CreationMonth','Type'])['Total'].idxmax()]
    m2 = m1[['CreationMonth','Type','Total']].copy()
    mx = m2.groupby(['Type', 'CreationMonth'])['Total'].sum().unstack(fill_value=0).reset_index().rename_axis(None, axis=1)
    return mx

#Get individual rating
def individualrating(mon, df, mx, rateoutput):
    for i in range(mon):
        i = i + 1
        cols = [i]
        func = np.vectorize(ratecal)
        month_rate = func(df[cols], mx[cols])
        rateoutput[cols] = month_rate


#Rating calculation
def ratecal(x,y):
    #x - individual count
    #y - highest count
    #Monthly high is zero i.e. no one has scored
    if y == 0:
        return 0

    # Monthly highest count <= 5, allot same rating as of score i.e. 1=1, 2=2...
    elif y <= 5:
        return x

    # Monthly highest count == individual count, rating 5
    elif x == y:
        return 5

    # Individual count < Monthly highest count, then calculate rating
    elif x < y:

        #Calcuate relative interval on the basis of highest scrore
        interval = round(y/5)
        #Define frequency distribution
        fd1 = 0 + interval

        if fd1 + 1 + interval < y:
            fd2 = fd1 + 1 + interval
        else:
            fd2 = y

        if fd2 + 1 + interval < y:
            fd3 = fd2 + 1 + interval
        else:
            fd3 = y

        if fd3 + 1 + interval < y:
            fd4 = fd3 + 1 + interval
        else:
            fd4 = y

        if fd4 + 1 + interval < y:
            fd5 = fd4 + 1 + interval
        else:
            fd5 = y

        #Compare individual score with frequency distribution and decide rating
        if 0 < x <= fd1:
            return 1
        else:
            if fd1 < x <= fd2:
                return 2
            else:
                if fd2 < x <= fd3:
                    return 3
                else:
                    if fd3 < x <= fd4:
                        return 4
                    else:
                        if fd4 < x <= fd5:
                            return 5
                        else:
                            return 0
