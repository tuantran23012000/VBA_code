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
        self.kho_nvl.insert(loc=0, column='Tháng', value=pd.DatetimeIndex(self.kho_nvl['Ngày nhập dữ liệu']).month)
        self.kho_nvl = self.kho_nvl.fillna('')
        #return self.kho_nvl

    def ma_nhap_kho(self):
        mnk = []
        for item in self.kho_nvl["Bút toán"]:
            if item=="NHẬP KHO NVL" or item=="NHẬP LẠI NVL TỪ RD" or item=="TỒN ĐẦU KỲ":
                mnk.append(1)
            else:
                mnk.append(0)
        self.kho_nvl.insert(loc=0, column='Mã nhập kho', value=mnk)
        self.kho_nvl = self.kho_nvl.fillna('')
        #return self.kho_nvl

    def dem_ma_so_trung(self):
        count=[]
        for i in range(1,len(self.kho_nvl["Mã NL/ số lô"])+1):
            if self.kho_nvl[i-1:i]['Mã NL/ số lô'][i-1]=="":
                count.append("")
            else:
                count.append((self.kho_nvl[0:i]['Mã NL/ số lô']==self.kho_nvl[i-1:i]['Mã NL/ số lô'][i-1]).value_counts()[True])
        self.kho_nvl['ĐẾM SỐ MÃ TRÙNG RD/SO LO']=count
        self.kho_nvl = self.kho_nvl.fillna('')

    def ma_check_trung_lon_nhat(self):
        s = pd.Series(self.kho_nvl["Mã NL/ số lô"].value_counts()).rename_axis('Mã NL/ số lô')
        s = s.reset_index(name='count')       
        self.kho_nvl['MÃ check trùng lớn nhất của mỗi lô']=self.kho_nvl.merge(s, on='Mã NL/ số lô', how='left')['count']
        self.kho_nvl = self.kho_nvl.fillna('')

    def ma_kh(self):
        tmp = self.kho_nvl.merge(self.dm_nvl,on="MÃ NVL",how='left')
        self.kho_nvl["Mã KH"] = tmp["MÃ NVL KH"] 
        self.kho_nvl = self.kho_nvl.fillna('')

    def phan_loai_nvl(self):
        tmp = self.kho_nvl.merge(self.dm_nvl,on="MÃ NVL",how='left')
        self.kho_nvl["Phân loại nvl"] = tmp["Phân loại"]
        self.kho_nvl = self.kho_nvl.fillna('')

    def tong_xuat_thuc(self):
        t1=(self.kho_nvl.where(self.kho_nvl["Mã KH"] !='').dropna()).where(self.kho_nvl["Bút toán"]=="XUẤT NVL ĐI SX" ).dropna().groupby(by=["Mã KH",'Mã PO/ DMUA',"Bút toán"])['Mã NL/ số lô'].count()
        t2=(self.kho_nvl.where(self.kho_nvl["Mã KH"] !='').dropna()).where(self.kho_nvl["Bút toán"]=="XUẤT NVL ĐI SƠ CHẾ" ).dropna().groupby(by=["Mã KH",'Mã PO/ DMUA',"Bút toán"])['Mã NL/ số lô'].count()
        t3=(self.kho_nvl.where(self.kho_nvl["Mã KH"] !='').dropna()).where(self.kho_nvl["Bút toán"]=="NHẬP LẠI NVL SAU SX" ).dropna().groupby(by=["Mã KH",'Mã PO/ DMUA',"Bút toán"])['Mã NL/ số lô'].count()
        self.kho_nvl = self.kho_nvl.merge(t1+t2-t3, on=["Mã KH",'Mã PO/ DMUA',"Bút toán"], how='left').fillna('').rename({"Mã NL/ số lô_y":"tổng xuất thực"},axis=1)
        self.kho_nvl['tổng xuất thực']=self.kho_nvl['tổng xuất thực'].replace('',0)

    def gia_mua(self):
        gia_dmua = []
        tmp3 = self.kho_nvl.merge(self.DMUA, on=["Mã PO/ DMUA"], how='left').fillna('')
        for i in range(self.kho_nvl['Bút toán'].size):
            if self.kho_nvl['Bút toán'][i] == "NHẬP LẠI NVL TỪ RD":
                gia_dmua.append(0)
            else:
                if self.kho_nvl['Mã nhập kho'][i] == 1:
                    gia_dmua.append(tmp3["Đơn giá KỲ NÀY"][tmp3[tmp3['Mã PO/ DMUA']==self.kho_nvl['Mã PO/ DMUA'][i]].index.values[0]])
                else:
                    gia_dmua.append(0)
        self.kho_nvl["Giá DMUA"] = gia_dmua
        self.kho_nvl['Giá DMUA']=self.kho_nvl['Giá DMUA'].replace('',0)

    def gia_trung_binh(self):
        tmp4 = pd.Series(self.kho_nvl.groupby(by="Mã NL/ số lô_x")['Giá DMUA'].sum()).reset_index(name='count')
        tmp5 = pd.Series(self.kho_nvl.groupby(by="Mã NL/ số lô_x")['Mã nhập kho'].sum()).reset_index(name='count')
        tmp4['Giá trung bình'] = tmp4['count']/tmp5['count']
        self.kho_nvl['Giá trung bình'] = self.kho_nvl.merge(tmp4, on=["Mã NL/ số lô_x"], how='left')['Giá trung bình']
        self.kho_nvl['Giá trung bình']=self.kho_nvl['Giá trung bình'].fillna('').replace('',0)
    
    def don_gia(self):
        dg = []
        for i in range(self.kho_nvl['Mã nhập kho'].size):
            if self.kho_nvl['Mã nhập kho'][i]==1:
                dg.append(int(self.kho_nvl['Giá DMUA'][i]))
            else:
                dg.append(int(self.kho_nvl['Giá trung bình'][i]))
        self.kho_nvl['Đơn giá']=dg
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def thanh_tien(self):
        self.kho_nvl['Thành tiền']=self.kho_nvl['Số lượng']*self.kho_nvl['Đơn giá']
        self.kho_nvl = self.kho_nvl.fillna('')

    def so_luong_tren_po(self):
        self.kho_nvl['Số lượng trên PO']=self.kho_nvl.merge(self.po,on='Mã PO/ DMUA',how='left')['Số lượng đặt (theo ĐV BTP1)'].fillna('').replace('',0)
        self.kho_nvl = self.kho_nvl.fillna('')

    def so_luong_lam_ra(self):
        tp=self.KHO_TP.merge(self.cap_ma_sp,on="MÃ SP",how='left')[["MÃ PO","BÚT TOÁN","Nhà phân phối đúng","MÃ SP/SÔ LÔ","MÃ SP"," SỐ LƯỢNG","Số BTP1/1TP ","Thể tích/ khối lượng thực (cả vỏ nang)","Số lượng BTP1/BTP2","Số lượng BTP2/BTP3"]]
        slqd=[]
        for i in range(len(tp["MÃ PO"])):
            if tp[" SỐ LƯỢNG"][i]=="":
                slqd.append(0)
            else:
                if tp["MÃ SP/SÔ LÔ"][i][-4:]=="BTP0":
                    slqd.append(tp[" SỐ LƯỢNG"][i]*1000.0/tp["Thể tích/ khối lượng thực (cả vỏ nang)"][i])
                elif tp["MÃ SP/SÔ LÔ"][i][-4:]=="BTP1":
                    slqd.append(tp[" SỐ LƯỢNG"][i])
                elif tp["MÃ SP/SÔ LÔ"][i][-4:]=="BTP2":
                    slqd.append(tp[" SỐ LƯỢNG"][i]*tp["Số lượng BTP1/BTP2"][i])
                elif tp["MÃ SP/SÔ LÔ"][i][-4:]=="BTP3":
                    slqd.append(tp[" SỐ LƯỢNG"][i]*tp["Số lượng BTP2/BTP3"][i]*tp["Số lượng BTP1/BTP2"][i])        
                else:
                    slqd.append(tp[" SỐ LƯỢNG"][i]*tp["Số BTP1/1TP "][i])
        tp.insert(loc=0, column='Số lượng tính qui đổi sang BTP1', value=slqd)
        tmp = tp.where(tp['BÚT TOÁN']=='NHẬP KHO TP SAU GIA CÔNG').where(tp['Nhà phân phối đúng']=='331PULIPHA').fillna('').replace('',0)
        tmp1 = tp.where(tp['BÚT TOÁN']=='NHẬP KHO TP TỪ SX').fillna('').replace('',0)
        tmp2=[]
        for i in range(self.kho_nvl['Mã PO/ DMUA'].size):
            c1=0
            c=0
            check = tmp[tmp['MÃ PO']==self.kho_nvl['Mã PO/ DMUA'][i]].index.values
            check1 = tmp1[tmp1['MÃ PO']==self.kho_nvl['Mã PO/ DMUA'][i]].index.values
            if len(check1)!=0:
                for k in check1:
                    c1+=tmp1['Số lượng tính qui đổi sang BTP1'][k]
            if len(check)!=0:
                for k in check:
                    c+=tmp['Số lượng tính qui đổi sang BTP1'][k]
            tmp2.append(c1+c)
        self.kho_nvl['số lượng làm ra được'] = tmp2
        self.kho_nvl = self.kho_nvl.fillna('')

    def dinh_muc(self):
        tmp = []
        for i in range(len(self.kho_nvl["Mã PO/ DMUA"])):
            tmp.append(self.kho_nvl["Mã PO/ DMUA"][i][0:4]+self.kho_nvl["Mã KH"][i])
        g1 = pd.DataFrame(tmp,columns=['MA SP&MAKH']).merge(self.DINH_MUC, on='MA SP&MAKH', how='left')['ĐM THEO ĐV KHO'].fillna('').replace('',0)
        dm = []
        for i in range(0,len(self.kho_nvl["Phân loại nvl"])):
            if self.kho_nvl["Phân loại nvl"][i]=="BBC1" or self.kho_nvl["Phân loại nvl"][i]=="BBC2":
                try:
                    check = self.kho_nvl['số lượng làm ra được'][i]*g1[i]
                    dm.append(check)
                except:
                    dm.append(0)
            else:
                try:
                    check = self.kho_nvl['Số lượng trên PO'][i]*g1[i]
                    dm.append(check)
                except:
                    dm.append(0)
        self.kho_nvl["định mức"] = dm
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def chenh(self):
        self.kho_nvl['Chênh']=self.kho_nvl['tổng xuất thực']-self.kho_nvl['định mức']
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def gt_kho(self):
        tmp = []
        for i in range(self.kho_nvl['Mã KH'].size):
            t=self.GT_SAILECH_KHO[self.GT_SAILECH_KHO['MÃ KIỂM TRA']==self.kho_nvl['Mã PO/ DMUA'][i]+self.kho_nvl['Mã KH'][i]].index.values
            if len(t)==0:
                tmp.append('')
            else:
                tmp.append(self.GT_SAILECH_KHO['Ghi chú theo qui định'][t[0]])
        self.kho_nvl['GT KHO'] = tmp
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def gt_rd(self):
        tmp = []
        for i in range(self.kho_nvl['Mã KH'].size):
            t=self.GT_SAILECH_RD[self.GT_SAILECH_RD['MÃ KIỂM TRA']==self.kho_nvl['Mã PO/ DMUA'][i]+self.kho_nvl['Mã KH'][i]].index.values
            if len(t)==0:
                tmp.append('')
            else:
                tmp.append(self.GT_SAILECH_RD['KQ'][t[0]])
        self.kho_nvl['GT RD'] = tmp
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def gt_qa(self):
        tmp = []
        for i in range(self.kho_nvl['Mã KH'].size):
            t=self.GT_QA[self.GT_QA['MÃ KIỂM TRA']==self.kho_nvl['Mã PO/ DMUA'][i]+self.kho_nvl['Mã KH'][i]].index.values
            if len(t)==0:
                tmp.append('')
            else:
                tmp.append(self.GT_QA['kết quả kiểm soát'][t[0]])
        self.kho_nvl['GT QA'] = tmp
        self.kho_nvl = self.kho_nvl.fillna('')

    def ma_rd_ton_thuc_te(self):
        rd_ton = []
        for i in range(0,len(self.kho_nvl['ĐẾM SỐ MÃ TRÙNG RD/SO LO'])):
            if self.kho_nvl['ĐẾM SỐ MÃ TRÙNG RD/SO LO'][i]==self.kho_nvl['MÃ check trùng lớn nhất của mỗi lô'][i] and self.kho_nvl["T"][i]!=0:
                rd_ton.append(self.kho_nvl['MÃ NVL'][i])
            else:
                rd_ton.append("")
        self.kho_nvl.insert(loc=0, column='mã rd tồn thực tế', value=rd_ton)
        self.kho_nvl = self.kho_nvl.fillna('')

    def ma_kh_ton_thuc_te(self):
        kh_ton = []
        for i in range(0,len(self.kho_nvl['ĐẾM SỐ MÃ TRÙNG RD/SO LO'])):
            if self.kho_nvl['ĐẾM SỐ MÃ TRÙNG RD/SO LO'][i]==self.kho_nvl['MÃ check trùng lớn nhất của mỗi lô'][i] and self.kho_nvl["T"][i]!=0:
                kh_ton.append(self.kho_nvl['MÃ NVL'][i])
            else:
                kh_ton.append("")
        self.kho_nvl.insert(loc=0, column='mã KH tồn thực tế', value=kh_ton)
        self.kho_nvl = self.kho_nvl.fillna('')

    def dem_ma_kh_ton_thuc_te(self):
        tmp=[]
        for i in range(1,len(self.kho_nvl['mã KH tồn thực tế'])+1):
            if self.kho_nvl['mã KH tồn thực tế'][i-1]=="":
                tmp.append("")
            else:
                tmp.append(str(self.kho_nvl['mã KH tồn thực tế'][i-1])+"."+str(self.kho_nvl[0:i].groupby(by="mã KH tồn thực tế")["mã KH tồn thực tế"].count()[str(self.kho_nvl['mã KH tồn thực tế'][i-1])]))
        self.kho_nvl.insert(loc=0, column='ĐẾM MÃ KH TỒN THỰC TẾ', value=tmp)
        self.kho_nvl = self.kho_nvl.fillna('')

    def ma_co(self):
        self.kho_nvl.insert(loc=0, column='MÃ CÓ', value=self.kho_nvl.merge(self.BUTTOAN,on='Bút toán',how='left')['TKCO'].fillna(''))
        self.kho_nvl = self.kho_nvl.fillna('')
    
    def ma_no(self):
        self.kho_nvl.insert(loc=0, column='MÃ NỢ', value=self.kho_nvl.merge(self.BUTTOAN,on='Bút toán',how='left')['TKNO'].fillna(''))
        self.kho_nvl = self.kho_nvl.fillna('')

    def stt_lech_dinh_muc(self):
        tmp = []
        for i in range(self.kho_nvl['Chênh'].size):
            if self.kho_nvl['Chênh'][i] == 0:
                tmp.append(0)
            else:
                tmp.append(1+max(tmp))
        self.kho_nvl.insert(loc=2, column='STT lệch so với DM', value=tmp)
        self.kho_nvl = self.kho_nvl.fillna('')

    def save(self,path):
        self.kho_nvl.to_excel(path)
        print("LƯU THÀNH CÔNG SHEET KHO_NVL !")
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
        kho_nvl_ton=self.kho_nvl.merge(self.dm_nvl,on='MÃ NVL',how="left")[['MÃ NỢ', 'MÃ CÓ','TÊN THƯƠNG MẠI', 'Tên nguyên liệu', 'NCC_y', 'ĐV lấy vào định mức',
       'ĐVT KHO', 'Số lần chuyển đổi từ đv Kho sang đv ĐM', 'Phân loại',
       'Tiêu chuẩn ', 'Nhà sản xuất', 'Xuất xứ', 'Packaging',
       'hệ số chuyển đổi giữa mã RD sang mã KH', 'hình ảnh',"T","Số lượng","MÃ NVL","MÃ NVL KH"]]
        kho_nvl_ton["MÃ NVL"]=kho_nvl_ton["MÃ NVL"].str.upper()
        kho_nvl_ton=kho_nvl_ton.where(kho_nvl_ton["MÃ NVL"]!="")
        kho_nvl_ton=kho_nvl_ton.dropna(how='all')
        for i in range (len(kho_nvl_ton["T"])):
            try:
                kho_nvl_ton["T"][i]=float(kho_nvl_ton["T"][i])
            except:
                kho_nvl_ton["T"][i]=0
        for i in range (len(kho_nvl_ton["Số lượng"])):
            try:
                kho_nvl_ton["Số lượng"][i]=float(kho_nvl_ton["Số lượng"][i])
            except:
                kho_nvl_ton["Số lượng"][i]=0
        for i in range (len(kho_nvl_ton["MÃ NỢ"])):
            kho_nvl_ton["MÃ NỢ"][i]=str(kho_nvl_ton["MÃ NỢ"][i])
            kho_nvl_ton["MÃ CÓ"][i]=str(kho_nvl_ton["MÃ CÓ"][i])
        tttt=kho_nvl_ton.groupby("MÃ NVL")["T"].sum()
        tn=kho_nvl_ton.where(kho_nvl_ton["MÃ NỢ"]=="1521").groupby("MÃ NVL")["Số lượng"].sum()
        tx=kho_nvl_ton.where(kho_nvl_ton["MÃ CÓ"]=="1521").groupby("MÃ NVL")["Số lượng"].sum()
        kho_nvl_ton_nl=kho_nvl_ton.merge(tttt,on="MÃ NVL",how="left").merge(tn,on="MÃ NVL",how="left").merge(tx,on="MÃ NVL",how="left")[['MÃ NVL',"MÃ NVL KH",'TÊN THƯƠNG MẠI', 'Tên nguyên liệu', 'NCC_y',
            'ĐV lấy vào định mức', 'ĐVT KHO',
            'Số lần chuyển đổi từ đv Kho sang đv ĐM', 'Phân loại', 'Tiêu chuẩn ',
            'Nhà sản xuất', 'Xuất xứ', 'Packaging',
            'hệ số chuyển đổi giữa mã RD sang mã KH', 'hình ảnh',"T_y","Số lượng_y","Số lượng"]]
        kho_nvl_ton_nl=kho_nvl_ton_nl.rename(columns={"T_y":"tổng tồn thực tế","Số lượng_y":"TỔNG NHẬP","Số lượng":"TỔNG XUẤT","NCC_y":"NCC"}).fillna("")
        kho_nvl_ton_nl["TỔNG NHẬP"]=kho_nvl_ton_nl["TỔNG NHẬP"].replace("",0)
        kho_nvl_ton_nl["TỔNG XUẤT"]=kho_nvl_ton_nl["TỔNG XUẤT"].replace("",0)
        kho_nvl_ton_nl.insert(loc=18, column='Hư hao', value=kho_nvl_ton_nl["TỔNG NHẬP"]-kho_nvl_ton_nl["TỔNG XUẤT"])
        kho_nvl_ton_nl = kho_nvl_ton_nl.drop_duplicates(subset='MÃ NVL')
        self.dm_nvl=self.dm_nvl.merge(kho_nvl_ton_nl,on=self.dm_nvl.columns.tolist(),how='left')
    
    def save(self,path):
        self.dm_nvl.to_excel(path)
        print("LƯU THÀNH CÔNG SHEET DM NVL(NL)-TON KHO !")

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
        self.xuat_huy_tp=self.xuat_huy_tp.merge(self.gia_tp, on='MÃ PO', how='left')[['THÁNG', 'MÃ PO', 'Ngày nhập dữ liệu', 'BUT TOAN', 'TKNO', 'TKCO','SOLUONG','GIÁ BÁN (ĐV TÍNH BTP1)']]
        self.xuat_huy_tp=self.xuat_huy_tp.rename(columns={"GIÁ BÁN (ĐV TÍNH BTP1)":"DONGIA"})
        self.xuat_huy_tp=self.xuat_huy_tp.fillna(0)
        self.xuat_huy_tp.insert(loc=8, column='THANHTIEN', value=self.xuat_huy_tp["SOLUONG"]*self.xuat_huy_tp["DONGIA"])
    
    def save(self,path):
        self.xuat_huy_tp.to_excel(path)
        print("LƯU THÀNH CÔNG SHEET XUAT HUY TP !")
