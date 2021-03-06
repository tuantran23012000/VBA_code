import sys
import os
import pandas as pd
import time
import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

class KHO_NVL():
    def __init__(self,kho_nvl,DINH_MUC,po,DMUA,GT_SAILECH_KHO,GT_SAILECH_RD,GT_QA,BUTTOAN,dm_nvl,cap_ma_sp,GT_SAILECH_TP,KHO_TP):
        GT_QA = GT_QA.fillna('')
        kho_nvl = kho_nvl.fillna('')
        DINH_MUC = DINH_MUC.fillna('')
        po = po.fillna('')
        DMUA = DMUA.fillna('')
        GT_SAILECH_TP = GT_SAILECH_TP.fillna('')
        GT_SAILECH_KHO = GT_SAILECH_KHO.fillna('')
        GT_SAILECH_RD = GT_SAILECH_RD.fillna('')
        BUTTOAN = BUTTOAN.fillna('')
        KHO_TP = KHO_TP.fillna('')
        dm_nvl = dm_nvl.fillna('')
        cap_ma_sp = cap_ma_sp.fillna('')
        kho_nvl = kho_nvl.applymap(lambda s: s.upper() if type(s) == str else s)
        DINH_MUC = DINH_MUC.applymap(lambda s: s.upper() if type(s) == str else s)
        po = po.applymap(lambda s: s.upper() if type(s) == str else s)
        dm_nvl = dm_nvl.applymap(lambda s: s.upper() if type(s) == str else s)
        DMUA = DMUA.applymap(lambda s: s.upper() if type(s) == str else s)
        GT_SAILECH_TP = GT_SAILECH_TP.applymap(lambda s: s.upper() if type(s) == str else s)
        GT_SAILECH_KHO = GT_SAILECH_KHO.applymap(lambda s: s.upper() if type(s) == str else s)
        GT_SAILECH_RD = GT_SAILECH_RD.applymap(lambda s: s.upper() if type(s) == str else s)
        BUTTOAN = BUTTOAN.applymap(lambda s: s.upper() if type(s) == str else s)
        GT_QA = GT_QA.applymap(lambda s: s.upper() if type(s) == str else s)
        KHO_TP = KHO_TP.applymap(lambda s: s.upper() if type(s) == str else s)
        cap_ma_sp = cap_ma_sp.applymap(lambda s: s.upper() if type(s) == str else s)
        self.kho_nvl = kho_nvl
        self.DINH_MUC = DINH_MUC
        self.po = po
        self.DMUA = DMUA
        self.GT_SAILECH_KHO = GT_SAILECH_KHO
        self.GT_SAILECH_RD = GT_SAILECH_RD
        self.GT_QA = GT_QA
        self.BUTTOAN = BUTTOAN
        self.dm_nvl = dm_nvl
        self.GT_SAILECH_TP = GT_SAILECH_TP
        self.KHO_TP = KHO_TP
        self.cap_ma_sp = cap_ma_sp

    def thang(self):
        self.kho_nvl.insert(loc=0, column='Th??ng', value=pd.DatetimeIndex(self.kho_nvl['Ng??y nh???p d??? li???u']).month)
        self.kho_nvl = self.kho_nvl.fillna('')
        #return self.kho_nvl

    def ma_nhap_kho(self):
        mnk = []
        for item in self.kho_nvl["B??t to??n"]:
            if item=="NH???P KHO NVL" or item=="NH???P L???I NVL T??? RD" or item=="T???N ?????U K???":
                mnk.append(1)
            else:
                mnk.append(0)
        self.kho_nvl.insert(loc=0, column='M?? nh???p kho', value=mnk)
        self.kho_nvl = self.kho_nvl.fillna('')
        #return self.kho_nvl

    def dem_ma_so_trung(self):
        count=[]
        for i in range(1,len(self.kho_nvl["M?? NL/ s??? l??"])+1):
            if self.kho_nvl[i-1:i]['M?? NL/ s??? l??'][i-1]=="":
                count.append("")
            else:
                count.append((self.kho_nvl[0:i]['M?? NL/ s??? l??']==self.kho_nvl[i-1:i]['M?? NL/ s??? l??'][i-1]).value_counts()[True])
        self.kho_nvl['?????M S??? M?? TR??NG RD/SO LO']=count
        self.kho_nvl = self.kho_nvl.fillna('')

    def ma_check_trung_lon_nhat(self):
        s = pd.Series(self.kho_nvl["M?? NL/ s??? l??"].value_counts()).rename_axis('M?? NL/ s??? l??')
        s = s.reset_index(name='count')       
        self.kho_nvl['M?? check tr??ng l???n nh???t c???a m???i l??']=self.kho_nvl.merge(s, on='M?? NL/ s??? l??', how='left')['count']
        self.kho_nvl = self.kho_nvl.fillna('')

    def ma_kh(self):
        tmp = self.kho_nvl.merge(self.dm_nvl,on="M?? NVL",how='left')
        self.kho_nvl["M?? KH"] = tmp["M?? NVL KH"] 
        self.kho_nvl = self.kho_nvl.fillna('')

    def phan_loai_nvl(self):
        tmp = self.kho_nvl.merge(self.dm_nvl,on="M?? NVL",how='left')
        self.kho_nvl["Ph??n lo???i nvl"] = tmp["Ph??n lo???i"]
        self.kho_nvl = self.kho_nvl.fillna('')

    def tong_xuat_thuc(self):
        t1=(self.kho_nvl.where(self.kho_nvl["M?? KH"] !='').dropna()).where(self.kho_nvl["B??t to??n"]=="XU???T NVL ??I SX" ).dropna().groupby(by=["M?? KH",'M?? PO/ DMUA',"B??t to??n"])['M?? NL/ s??? l??'].count()
        t2=(self.kho_nvl.where(self.kho_nvl["M?? KH"] !='').dropna()).where(self.kho_nvl["B??t to??n"]=="XU???T NVL ??I S?? CH???" ).dropna().groupby(by=["M?? KH",'M?? PO/ DMUA',"B??t to??n"])['M?? NL/ s??? l??'].count()
        t3=(self.kho_nvl.where(self.kho_nvl["M?? KH"] !='').dropna()).where(self.kho_nvl["B??t to??n"]=="NH???P L???I NVL SAU SX" ).dropna().groupby(by=["M?? KH",'M?? PO/ DMUA',"B??t to??n"])['M?? NL/ s??? l??'].count()
        self.kho_nvl = self.kho_nvl.merge(t1+t2-t3, on=["M?? KH",'M?? PO/ DMUA',"B??t to??n"], how='left').fillna('').rename({"M?? NL/ s??? l??_y":"t???ng xu???t th???c"},axis=1)
        self.kho_nvl['t???ng xu???t th???c']=self.kho_nvl['t???ng xu???t th???c'].replace('',0)

    def gia_mua(self):
        gia_dmua = []
        tmp3 = self.kho_nvl.merge(self.DMUA, on=["M?? PO/ DMUA"], how='left').fillna('')
        for i in range(self.kho_nvl['B??t to??n'].size):
            if self.kho_nvl['B??t to??n'][i] == "NH???P L???I NVL T??? RD":
                gia_dmua.append(0)
            else:
                if self.kho_nvl['M?? nh???p kho'][i] == 1:
                    gia_dmua.append(tmp3["????n gi?? K??? N??Y"][tmp3[tmp3['M?? PO/ DMUA']==self.kho_nvl['M?? PO/ DMUA'][i]].index.values[0]])
                else:
                    gia_dmua.append(0)
        self.kho_nvl["Gi?? DMUA"] = gia_dmua
        self.kho_nvl['Gi?? DMUA']=self.kho_nvl['Gi?? DMUA'].replace('',0)

    def gia_trung_binh(self):
        tmp4 = pd.Series(self.kho_nvl.groupby(by="M?? NL/ s??? l??_x")['Gi?? DMUA'].sum()).reset_index(name='count')
        tmp5 = pd.Series(self.kho_nvl.groupby(by="M?? NL/ s??? l??_x")['M?? nh???p kho'].sum()).reset_index(name='count')
        tmp4['Gi?? trung b??nh'] = tmp4['count']/tmp5['count']
        self.kho_nvl['Gi?? trung b??nh'] = self.kho_nvl.merge(tmp4, on=["M?? NL/ s??? l??_x"], how='left')['Gi?? trung b??nh']
        self.kho_nvl['Gi?? trung b??nh']=self.kho_nvl['Gi?? trung b??nh'].fillna('').replace('',0)
    
    def don_gia(self):
        dg = []
        for i in range(self.kho_nvl['M?? nh???p kho'].size):
            if self.kho_nvl['M?? nh???p kho'][i]==1:
                dg.append(int(self.kho_nvl['Gi?? DMUA'][i]))
            else:
                dg.append(int(self.kho_nvl['Gi?? trung b??nh'][i]))
        self.kho_nvl['????n gi??']=dg
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def thanh_tien(self):
        self.kho_nvl['Th??nh ti???n']=self.kho_nvl['S??? l?????ng']*self.kho_nvl['????n gi??']
        self.kho_nvl = self.kho_nvl.fillna('')

    def so_luong_tren_po(self):
        self.kho_nvl['S??? l?????ng tr??n PO']=self.kho_nvl.merge(self.po,on='M?? PO/ DMUA',how='left')['S??? l?????ng ?????t (theo ??V BTP1)'].fillna('').replace('',0)
        self.kho_nvl = self.kho_nvl.fillna('')

    def so_luong_lam_ra(self):
        tp=self.KHO_TP.merge(self.cap_ma_sp,on="M?? SP",how='left')[["M?? PO","B??T TO??N","Nh?? ph??n ph???i ????ng","M?? SP/S?? L??","M?? SP"," S??? L?????NG","S??? BTP1/1TP ","Th??? t??ch/ kh???i l?????ng th???c (c??? v??? nang)","S??? l?????ng BTP1/BTP2","S??? l?????ng BTP2/BTP3"]]
        slqd=[]
        for i in range(len(tp["M?? PO"])):
            if tp[" S??? L?????NG"][i]=="":
                slqd.append(0)
            else:
                if tp["M?? SP/S?? L??"][i][-4:]=="BTP0":
                    slqd.append(tp[" S??? L?????NG"][i]*1000.0/tp["Th??? t??ch/ kh???i l?????ng th???c (c??? v??? nang)"][i])
                elif tp["M?? SP/S?? L??"][i][-4:]=="BTP1":
                    slqd.append(tp[" S??? L?????NG"][i])
                elif tp["M?? SP/S?? L??"][i][-4:]=="BTP2":
                    slqd.append(tp[" S??? L?????NG"][i]*tp["S??? l?????ng BTP1/BTP2"][i])
                elif tp["M?? SP/S?? L??"][i][-4:]=="BTP3":
                    slqd.append(tp[" S??? L?????NG"][i]*tp["S??? l?????ng BTP2/BTP3"][i]*tp["S??? l?????ng BTP1/BTP2"][i])        
                else:
                    slqd.append(tp[" S??? L?????NG"][i]*tp["S??? BTP1/1TP "][i])
        tp.insert(loc=0, column='S??? l?????ng t??nh qui ?????i sang BTP1', value=slqd)
        tmp = tp.where(tp['B??T TO??N']=='NH???P KHO TP SAU GIA C??NG').where(tp['Nh?? ph??n ph???i ????ng']=='331PULIPHA').fillna('').replace('',0)
        tmp1 = tp.where(tp['B??T TO??N']=='NH???P KHO TP T??? SX').fillna('').replace('',0)
        tmp2=[]
        for i in range(self.kho_nvl['M?? PO/ DMUA'].size):
            c1=0
            c=0
            check = tmp[tmp['M?? PO']==self.kho_nvl['M?? PO/ DMUA'][i]].index.values
            check1 = tmp1[tmp1['M?? PO']==self.kho_nvl['M?? PO/ DMUA'][i]].index.values
            if len(check1)!=0:
                for k in check1:
                    c1+=tmp1['S??? l?????ng t??nh qui ?????i sang BTP1'][k]
            if len(check)!=0:
                for k in check:
                    c+=tmp['S??? l?????ng t??nh qui ?????i sang BTP1'][k]
            tmp2.append(c1+c)
        self.kho_nvl['s??? l?????ng l??m ra ???????c'] = tmp2
        self.kho_nvl = self.kho_nvl.fillna('')

    def dinh_muc(self):
        tmp = []
        for i in range(len(self.kho_nvl["M?? PO/ DMUA"])):
            tmp.append(self.kho_nvl["M?? PO/ DMUA"][i][0:4]+self.kho_nvl["M?? KH"][i])
        g1 = pd.DataFrame(tmp,columns=['MA SP&MAKH']).merge(self.DINH_MUC, on='MA SP&MAKH', how='left')['??M THEO ??V KHO'].fillna('').replace('',0)
        dm = []
        for i in range(0,len(self.kho_nvl["Ph??n lo???i nvl"])):
            if self.kho_nvl["Ph??n lo???i nvl"][i]=="BBC1" or self.kho_nvl["Ph??n lo???i nvl"][i]=="BBC2":
                try:
                    check = self.kho_nvl['s??? l?????ng l??m ra ???????c'][i]*g1[i]
                    dm.append(check)
                except:
                    dm.append(0)
            else:
                try:
                    check = self.kho_nvl['S??? l?????ng tr??n PO'][i]*g1[i]
                    dm.append(check)
                except:
                    dm.append(0)
        self.kho_nvl["?????nh m???c"] = dm
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def chenh(self):
        self.kho_nvl['Ch??nh']=self.kho_nvl['t???ng xu???t th???c']-self.kho_nvl['?????nh m???c']
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def gt_kho(self):
        tmp = []
        for i in range(self.kho_nvl['M?? KH'].size):
            t=self.GT_SAILECH_KHO[self.GT_SAILECH_KHO['M?? KI???M TRA']==self.kho_nvl['M?? PO/ DMUA'][i]+self.kho_nvl['M?? KH'][i]].index.values
            if len(t)==0:
                tmp.append('')
            else:
                tmp.append(self.GT_SAILECH_KHO['Ghi ch?? theo qui ?????nh'][t[0]])
        self.kho_nvl['GT KHO'] = tmp
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def gt_rd(self):
        tmp = []
        for i in range(self.kho_nvl['M?? KH'].size):
            t=self.GT_SAILECH_RD[self.GT_SAILECH_RD['M?? KI???M TRA']==self.kho_nvl['M?? PO/ DMUA'][i]+self.kho_nvl['M?? KH'][i]].index.values
            if len(t)==0:
                tmp.append('')
            else:
                tmp.append(self.GT_SAILECH_RD['KQ'][t[0]])
        self.kho_nvl['GT RD'] = tmp
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def gt_qa(self):
        tmp = []
        for i in range(self.kho_nvl['M?? KH'].size):
            t=self.GT_QA[self.GT_QA['M?? KI???M TRA']==self.kho_nvl['M?? PO/ DMUA'][i]+self.kho_nvl['M?? KH'][i]].index.values
            if len(t)==0:
                tmp.append('')
            else:
                tmp.append(self.GT_QA['k???t qu??? ki???m so??t'][t[0]])
        self.kho_nvl['GT QA'] = tmp
        self.kho_nvl = self.kho_nvl.fillna('')

    def ma_rd_ton_thuc_te(self):
        rd_ton = []
        for i in range(0,len(self.kho_nvl['?????M S??? M?? TR??NG RD/SO LO'])):
            if self.kho_nvl['?????M S??? M?? TR??NG RD/SO LO'][i]==self.kho_nvl['M?? check tr??ng l???n nh???t c???a m???i l??'][i] and self.kho_nvl["T"][i]!=0:
                rd_ton.append(self.kho_nvl['M?? NVL'][i])
            else:
                rd_ton.append("")
        self.kho_nvl.insert(loc=0, column='m?? rd t???n th???c t???', value=rd_ton)
        self.kho_nvl = self.kho_nvl.fillna('')

    def ma_kh_ton_thuc_te(self):
        kh_ton = []
        for i in range(0,len(self.kho_nvl['?????M S??? M?? TR??NG RD/SO LO'])):
            if self.kho_nvl['?????M S??? M?? TR??NG RD/SO LO'][i]==self.kho_nvl['M?? check tr??ng l???n nh???t c???a m???i l??'][i] and self.kho_nvl["T"][i]!=0:
                kh_ton.append(self.kho_nvl['M?? NVL'][i])
            else:
                kh_ton.append("")
        self.kho_nvl.insert(loc=0, column='m?? KH t???n th???c t???', value=kh_ton)
        self.kho_nvl = self.kho_nvl.fillna('')

    def dem_ma_kh_ton_thuc_te(self):
        tmp=[]
        for i in range(1,len(self.kho_nvl['m?? KH t???n th???c t???'])+1):
            if self.kho_nvl['m?? KH t???n th???c t???'][i-1]=="":
                tmp.append("")
            else:
                tmp.append(str(self.kho_nvl['m?? KH t???n th???c t???'][i-1])+"."+str(self.kho_nvl[0:i].groupby(by="m?? KH t???n th???c t???")["m?? KH t???n th???c t???"].count()[str(self.kho_nvl['m?? KH t???n th???c t???'][i-1])]))
        self.kho_nvl.insert(loc=0, column='?????M M?? KH T???N TH???C T???', value=tmp)
        self.kho_nvl = self.kho_nvl.fillna('')

    def ma_co(self):
        self.kho_nvl.insert(loc=0, column='M?? C??', value=self.kho_nvl.merge(self.BUTTOAN,on='B??t to??n',how='left')['TKCO'].fillna(''))
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def ma_no(self):
        self.kho_nvl.insert(loc=0, column='M?? N???', value=self.kho_nvl.merge(self.BUTTOAN,on='B??t to??n',how='left')['TKNO'].fillna(''))
        self.kho_nvl = self.kho_nvl.fillna('')

    def stt_lech_dinh_muc(self):
        tmp = []
        for i in range(self.kho_nvl['Ch??nh'].size):
            if self.kho_nvl['Ch??nh'][i] == 0:
                tmp.append(0)
            else:
                tmp.append(1+max(tmp))
        self.kho_nvl.insert(loc=2, column='STT l???ch so v???i DM', value=tmp)
        self.kho_nvl = self.kho_nvl.fillna('')

    def save(self,path):
        self.kho_nvl.to_excel(path)
        print("L??U TH??NH C??NG SHEET KHO_NVL !")
        start = time.time()

class DM_NVL_NL_TON_KHO():
    def __init__(self,kho_nvl,dm_nvl):
        kho_nvl = kho_nvl.fillna('')
        dm_nvl = dm_nvl.fillna('')
        kho_nvl = kho_nvl.applymap(lambda s: s.upper() if type(s) == str else s)  
        dm_nvl = dm_nvl.applymap(lambda s: s.upper() if type(s) == str else s)
        self.kho_nvl = kho_nvl
        self.dm_nvl = dm_nvl
    
    def create(self):
        kho_nvl_ton=self.kho_nvl.merge(self.dm_nvl,on='M?? NVL',how="left")[['M?? N???', 'M?? C??','T??N TH????NG M???I', 'T??n nguy??n li???u', 'NCC_y', '??V l???y v??o ?????nh m???c',
       '??VT KHO', 'S??? l???n chuy???n ?????i t??? ??v Kho sang ??v ??M', 'Ph??n lo???i',
       'Ti??u chu???n ', 'Nh?? s???n xu???t', 'Xu???t x???', 'Packaging',
       'h??? s??? chuy???n ?????i gi???a m?? RD sang m?? KH', 'h??nh ???nh',"T","S??? l?????ng","M?? NVL","M?? NVL KH"]]
        kho_nvl_ton["M?? NVL"]=kho_nvl_ton["M?? NVL"].str.upper()
        kho_nvl_ton=kho_nvl_ton.where(kho_nvl_ton["M?? NVL"]!="")
        kho_nvl_ton=kho_nvl_ton.dropna(how='all')
        for i in range (len(kho_nvl_ton["T"])):
            try:
                kho_nvl_ton["T"][i]=float(kho_nvl_ton["T"][i])
            except:
                kho_nvl_ton["T"][i]=0
        for i in range (len(kho_nvl_ton["S??? l?????ng"])):
            try:
                kho_nvl_ton["S??? l?????ng"][i]=float(kho_nvl_ton["S??? l?????ng"][i])
            except:
                kho_nvl_ton["S??? l?????ng"][i]=0
        for i in range (len(kho_nvl_ton["M?? N???"])):
            kho_nvl_ton["M?? N???"][i]=str(kho_nvl_ton["M?? N???"][i])
            kho_nvl_ton["M?? C??"][i]=str(kho_nvl_ton["M?? C??"][i])
        tttt=kho_nvl_ton.groupby("M?? NVL")["T"].sum()
        tn=kho_nvl_ton.where(kho_nvl_ton["M?? N???"]=="1521").groupby("M?? NVL")["S??? l?????ng"].sum()
        tx=kho_nvl_ton.where(kho_nvl_ton["M?? C??"]=="1521").groupby("M?? NVL")["S??? l?????ng"].sum()
        kho_nvl_ton_nl=kho_nvl_ton.merge(tttt,on="M?? NVL",how="left").merge(tn,on="M?? NVL",how="left").merge(tx,on="M?? NVL",how="left")[['M?? NVL',"M?? NVL KH",'T??N TH????NG M???I', 'T??n nguy??n li???u', 'NCC_y',
            '??V l???y v??o ?????nh m???c', '??VT KHO',
            'S??? l???n chuy???n ?????i t??? ??v Kho sang ??v ??M', 'Ph??n lo???i', 'Ti??u chu???n ',
            'Nh?? s???n xu???t', 'Xu???t x???', 'Packaging',
            'h??? s??? chuy???n ?????i gi???a m?? RD sang m?? KH', 'h??nh ???nh',"T_y","S??? l?????ng_y","S??? l?????ng"]]
        kho_nvl_ton_nl=kho_nvl_ton_nl.rename(columns={"T_y":"t???ng t???n th???c t???","S??? l?????ng_y":"T???NG NH???P","S??? l?????ng":"T???NG XU???T","NCC_y":"NCC"}).fillna("")
        kho_nvl_ton_nl["T???NG NH???P"]=kho_nvl_ton_nl["T???NG NH???P"].replace("",0)
        kho_nvl_ton_nl["T???NG XU???T"]=kho_nvl_ton_nl["T???NG XU???T"].replace("",0)
        kho_nvl_ton_nl.insert(loc=18, column='H?? hao', value=kho_nvl_ton_nl["T???NG NH???P"]-kho_nvl_ton_nl["T???NG XU???T"])
        kho_nvl_ton_nl = kho_nvl_ton_nl.drop_duplicates(subset='M?? NVL')
        self.dm_nvl=self.dm_nvl.merge(kho_nvl_ton_nl,on=self.dm_nvl.columns.tolist(),how='left')
    
    def save(self,path):
        self.dm_nvl.to_excel(path)
        print("L??U TH??NH C??NG SHEET DM NVL(NL)-TON KHO !")

class XUAT_HUY_TP():
    def __init__(self,gia_tp,xuat_huy_tp):
        xuat_huy_tp = xuat_huy_tp.dropna(how="all")
        gia_tp=gia_tp.dropna(how="all")
        gia_tp = gia_tp.applymap(lambda s: s.upper() if type(s) == str else s)
        xuat_huy_tp = xuat_huy_tp.applymap(lambda s: s.upper() if type(s) == str else s)
        gia_tp=gia_tp.drop(gia_tp.columns[[0,1,2,3,4,5,6,7]],axis=1)
        new_header=gia_tp.iloc[1]
        gia_tp=gia_tp[2:]
        gia_tp.columns=new_header
        self.gia_tp = gia_tp
        self.xuat_huy_tp = xuat_huy_tp
    
    def create(self):
        self.xuat_huy_tp=self.xuat_huy_tp.merge(self.gia_tp, on='M?? PO', how='left')[['TH??NG', 'M?? PO', 'Ng??y nh???p d??? li???u', 'BUT TOAN', 'TKNO', 'TKCO','SOLUONG','GI?? B??N (??V T??NH BTP1)']]
        self.xuat_huy_tp=self.xuat_huy_tp.rename(columns={"GI?? B??N (??V T??NH BTP1)":"DONGIA"})
        self.xuat_huy_tp=self.xuat_huy_tp.fillna(0)
        self.xuat_huy_tp.insert(loc=8, column='THANHTIEN', value=self.xuat_huy_tp["SOLUONG"]*self.xuat_huy_tp["DONGIA"])
    
    def save(self,path):
        self.xuat_huy_tp.to_excel(path)
        print("L??U TH??NH C??NG SHEET XUAT HUY TP !")
