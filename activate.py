import sys
import os
import pandas as pd
from pandas import ExcelWriter
import time
from Center import KHO_NVL, DM_NVL_NL_TON_KHO, XUAT_HUY_TP

os.chdir(sys.path[0])
try:
    phan_mem_kho_nvl = pd.ExcelFile('//medzavy.synology.me/Python/Tuan/data/PHAN MEM KHO NVL.xlsm')
    phan_mem_kh = pd.ExcelFile('//medzavy.synology.me/Python/Tuan/data/PHAN MEM KH.xlsm')
    phan_mem_kho_tp = pd.ExcelFile('//medzavy.synology.me/Python/Tuan/data/PHAN MEM KHO THANH PHAM.xlsm')
    phan_mem_rd = pd.ExcelFile('//medzavy.synology.me/Python/Tuan/data/PHAN MEM RD.xlsm')
    cap_ma_kh_ncc_buttoan = pd.ExcelFile('//medzavy.synology.me/Python/Tuan/data/CAP MA KH-NCC-BUTTOAN.xlsm')    
    cap_ma_nvl = pd.ExcelFile('//medzavy.synology.me/Python/Tuan/data/CAP MA NVL.xlsm')
    gia_tp = pd.read_excel('//medzavy.synology.me/Python/Tuan/data/PHAN MEM KT.xlsm')
    GT_QA = pd.read_excel('//medzavy.synology.me/Python/Tuan/data/PHAN MEM QA.xlsm')
    po = pd.read_excel('//medzavy.synology.me/Python/Tuan/data/DANH MUC PO.xlsm')
    cap_ma_sp = pd.read_excel('//medzavy.synology.me/Python/Tuan/data/CAP MA SP.xlsm')
    DMUA = pd.read_excel(phan_mem_kh,'DMUA')
    KHO_TP=pd.read_excel(phan_mem_kho_tp,'KHO TP')
    GT_SAILECH_TP = pd.read_excel(phan_mem_kho_tp,'GIAI TRINH SAI LECH')
    GT_SAILECH_KHO = pd.read_excel(phan_mem_kho_nvl,'KHO GIAI TRINH')
    BUTTOAN = pd.read_excel(cap_ma_kh_ncc_buttoan,'BUT TOAN')
    dm_nvl = pd.read_excel(cap_ma_nvl,'DM NVL(NL)')
    DINH_MUC = pd.read_excel(phan_mem_rd,'DINH MUC')
    GT_SAILECH_RD = pd.read_excel(phan_mem_rd,'GT SAI LECH RD')
    kho_nvl = pd.read_excel(phan_mem_kho_nvl,'KHO NVL')
    xuat_huy_tp=pd.read_excel(phan_mem_kho_tp,'XUAT HUY TP')

    # KHO_NVL
    start1 = time.time()
    KHO = KHO_NVL(kho_nvl,DINH_MUC,po,DMUA,GT_SAILECH_KHO,GT_SAILECH_RD,GT_QA,BUTTOAN,dm_nvl,cap_ma_sp,GT_SAILECH_TP,KHO_TP)
    KHO.thang()
    KHO.ma_nhap_kho()
    KHO.dem_ma_so_trung()
    KHO.ma_check_trung_lon_nhat()
    KHO.ma_kh()
    KHO.phan_loai_nvl()
    KHO.tong_xuat_thuc()
    KHO.gia_mua()
    KHO.gia_trung_binh()
    KHO.don_gia()
    KHO.thanh_tien()
    KHO.so_luong_tren_po()
    KHO.so_luong_lam_ra()
    KHO.dinh_muc()
    KHO.chenh()
    KHO.gt_kho()
    KHO.gt_rd()
    KHO.gt_qa()
    KHO.ma_rd_ton_thuc_te()
    KHO.ma_kh_ton_thuc_te()
    KHO.dem_ma_kh_ton_thuc_te()
    KHO.ma_co()
    KHO.ma_no()
    KHO.stt_lech_dinh_muc()
    KHO.save('//medzavy.synology.me/Python/Tuan/results/KHO_NVL.xlsx')
    end1 = time.time()
    print("Thời gian xử lý sheet KHO_NVL: "+str(end1-start1)+"s")
    s1 = time.time()
    while True:
        e1 = time.time()
        if int(e1-s1) ==5:
            break

    # DM NVL(NL)-TON KHO
    start2 = time.time()
    KHO1 = DM_NVL_NL_TON_KHO(KHO.kho_nvl,dm_nvl)
    KHO1.create()
    KHO1.save('//medzavy.synology.me/Python/Tuan/results/DM NVL(NL)-TON KHO.xlsx')
    end2 = time.time()
    print("Thời gian xử lý sheet DM NVL(NL)-TON KHO: "+str(end2-start2)+"s")
    s2 = time.time()
    while True:
        e2 = time.time()
        if int(e2-s2) ==5:
            break

    # XUAT HUY TP
    start3 = time.time()
    KHO2 = XUAT_HUY_TP(gia_tp,xuat_huy_tp)
    KHO2.create()
    KHO2.save('//medzavy.synology.me/Python/Tuan/results/XUAT HUY TP.xlsx')
    end3 = time.time()
    print("Thời gian xử lý sheet XUAT HUY TP: "+str(end3-start3)+"s")
    s3 = time.time()
    while True:
        e3 = time.time()
        if int(e3-s3) ==5:
            break

        writer = pd.ExcelWriter('//medzavy.synology.me/Python/Tuan/results/trungtam.xlsx', engine='xlsxwriter')
        KHO.kho_nvl.to_excel(writer, sheet_name='KHO NVL')
        KHO1.dm_nvl.to_excel(writer, sheet_name='DM NVL(NL)-TON KHO')
        KHO2.xuat_huy_tp.to_excel(writer, sheet_name='XUAT HUY TP')
        writer.save()
except:
    print("Có lỗi !")
    s4 = time.time()
    while True:
        e4 = time.time()
        if int(e4-s4) ==15:
            break