"""
REDCap Data Loader
ดึงข้อมูลจาก REDCap API และโหลดลงฐานข้อมูลปลายทาง
"""
import asyncio
import logging
from typing import List, Dict, Any
import mysql.connector
from redcap import Project
import os
from datetime import datetime

logger = logging.getLogger(__name__)

# Pre-defined CREATE TABLE statement (no row size error)
CREATE_TABLE_SQL = """CREATE TABLE `{table_name}` (
	`id` INT(11) NOT NULL AUTO_INCREMENT,
	`record_id` VARCHAR(255) NULL DEFAULT NULL COMMENT 'รหัสผู้ป่วย' COLLATE 'utf8_general_ci',
	`redcap_event_name` TEXT NULL DEFAULT NULL COMMENT 'ชื่อการตรวจตามเหตุการณ์ของ REDCap' COLLATE 'utf8_general_ci',
	`redcap_repeat_instrument` TEXT NULL DEFAULT NULL COMMENT 'ชื่อเครื่องมือที่ใช้ในการตรวจซ้ำ' COLLATE 'utf8_general_ci',
	`redcap_repeat_instance` TEXT NULL DEFAULT NULL COMMENT 'จำนวนครั้งที่ตรวจซ้ำ' COLLATE 'utf8_general_ci',
	`institute` TEXT NULL DEFAULT NULL COMMENT 'สถาบันหรือโรงพยาบาล' COLLATE 'utf8_general_ci',
	`hn` VARCHAR(255) NULL DEFAULT NULL COMMENT 'หมายเลขผู้ป่วย' COLLATE 'utf8_general_ci',
	`an` TEXT NULL DEFAULT NULL COMMENT 'หมายเลขผู้ป่วยในหอผู้ป่วยวิกฤต' COLLATE 'utf8_general_ci',
	`icu_kku` TEXT NULL DEFAULT NULL COMMENT 'หมายเลขสำหรับหอผู้ป่วยวิกฤตของ KKU' COLLATE 'utf8_general_ci',
	`national_id` TEXT NULL DEFAULT NULL COMMENT 'เลขประจำตัวประชาชน' COLLATE 'utf8_general_ci',
	`dob_date` TEXT NULL DEFAULT NULL COMMENT 'วันเดือนปีเกิด' COLLATE 'utf8_general_ci',
	`h_adm_date` TEXT NULL DEFAULT NULL COMMENT 'วันที่รับเข้าโรงพยาบาล' COLLATE 'utf8_general_ci',
	`icu_adm_date` TEXT NULL DEFAULT NULL COMMENT 'วันที่รับเข้าหอผู้ป่วยวิกฤต' COLLATE 'utf8_general_ci',
	`sex` TEXT NULL DEFAULT NULL COMMENT 'เพศ' COLLATE 'utf8_general_ci',
	`age` TEXT NULL DEFAULT NULL COMMENT 'อายุ' COLLATE 'utf8_general_ci',
	`time_to_icu` TEXT NULL DEFAULT NULL COMMENT 'ระยะเวลาจากรับเข้าโรงพยาบาลถึงรับเข้าหอผู้ป่วยวิกฤต' COLLATE 'utf8_general_ci',
	`pdx` TEXT NULL DEFAULT NULL COMMENT 'การวินิจฉัยหลัก' COLLATE 'utf8_general_ci',
	`reason_icu` TEXT NULL DEFAULT NULL COMMENT 'สาเหตุที่เข้ารับการรักษาในหอผู้ป่วยวิกฤต' COLLATE 'utf8_general_ci',
	`insurance` TEXT NULL DEFAULT NULL COMMENT 'ประเภทประกันสุขภาพ' COLLATE 'utf8_general_ci',
	`insurance_oth` TEXT NULL DEFAULT NULL COMMENT 'ประกันสุขภาพอื่นๆ' COLLATE 'utf8_general_ci',
	`postcode` VARCHAR(255) NULL DEFAULT NULL COMMENT 'รหัสไปรษณีย์' COLLATE 'utf8_general_ci',
	`weight` TEXT NULL DEFAULT NULL COMMENT 'น้ำหนัก' COLLATE 'utf8_general_ci',
	`height` TEXT NULL DEFAULT NULL COMMENT 'ส่วนสูง' COLLATE 'utf8_general_ci',
	`bmi` TEXT NULL DEFAULT NULL COMMENT 'ดัชนีมวลกาย' COLLATE 'utf8_general_ci',
	`pregnancy` TEXT NULL DEFAULT NULL COMMENT 'ตั้งครรภ์' COLLATE 'utf8_general_ci',
	`gestatiion_wk` TEXT NULL DEFAULT NULL COMMENT 'อายุครรภ์' COLLATE 'utf8_general_ci',
	`cr_yn` TEXT NULL DEFAULT NULL COMMENT 'ผู้ป่วยมีโรคไตหรือไม่' COLLATE 'utf8_general_ci',
	`ckd` TEXT NULL DEFAULT NULL COMMENT 'โรคไตเรื้อรัง' COLLATE 'utf8_general_ci',
	`ckd_stage` TEXT NULL DEFAULT NULL COMMENT 'ระยะของโรคไตเรื้อรัง' COLLATE 'utf8_general_ci',
	`cr_baseline` TEXT NULL DEFAULT NULL COMMENT 'ค่าครีเอทินินพื้นฐาน' COLLATE 'utf8_general_ci',
	`cr_mdrd` TEXT NULL DEFAULT NULL COMMENT 'ค่าอัตราการกรองของไต (MDRD)' COLLATE 'utf8_general_ci',
	`egfr` TEXT NULL DEFAULT NULL COMMENT 'อัตราการกรองของไต (eGFR)' COLLATE 'utf8_general_ci',
	`h_adm_source` TEXT NULL DEFAULT NULL COMMENT 'แหล่งที่มารับเข้าโรงพยาบาล' COLLATE 'utf8_general_ci',
	`icu_adm_source` TEXT NULL DEFAULT NULL COMMENT 'แหล่งที่มารับเข้าหอผู้ป่วยวิกฤต' COLLATE 'utf8_general_ci',
	`elective_surg` TEXT NULL DEFAULT NULL COMMENT 'ผ่าตัดแบบมีนัด' COLLATE 'utf8_general_ci',
	`icu_adm_plan` TEXT NULL DEFAULT NULL COMMENT 'แผนการรับเข้าหอผู้ป่วยวิกฤต' COLLATE 'utf8_general_ci',
	`treat_goal` TEXT NULL DEFAULT NULL COMMENT 'เป้าหมายการรักษา' COLLATE 'utf8_general_ci',
	`arrest_bf` TEXT NULL DEFAULT NULL COMMENT 'ประวัติหยุดหายใจก่อนเข้ารับการรักษา' COLLATE 'utf8_general_ci',
	`demographic_complete` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___1` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___2` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___3` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___4` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___5` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___6` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___7` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___8` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___9` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___10` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___11` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___12` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___13` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___14` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___15` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___16` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___17` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___18` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___19` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___0` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci___na` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cci_age` TEXT NULL DEFAULT NULL COMMENT 'อายุใน CCI' COLLATE 'utf8_general_ci',
	`cci_total` TEXT NULL DEFAULT NULL COMMENT 'คะแนน CCI รวม' COLLATE 'utf8_general_ci',
	`charlson_comorbidity_index_complete` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`age_point_ap3` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`age_point_sap` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`comorbid_ap3___1` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`comorbid_ap3___2` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`comorbid_ap3___3` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`comorbid_ap3___4` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`comorbid_ap3___5` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`comorbid_ap3___6` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`comorbid_ap3___7` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`comorbid_ap3___0` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`comorbid_ap3___na` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`comorbid_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Comorbid จาก APACHE III' COLLATE 'utf8_general_ci',
	`comorbid_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Comorbid จาก SAPS' COLLATE 'utf8_general_ci',
	`adm_type` TEXT NULL DEFAULT NULL COMMENT 'ประเภทการรับไว้ในโรงพยาบาล' COLLATE 'utf8_general_ci',
	`adm_type_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Admission type จาก SAPS' COLLATE 'utf8_general_ci',
	`arrest_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Arrest จาก SAPS' COLLATE 'utf8_general_ci',
	`ap_bt_high` TEXT NULL DEFAULT NULL COMMENT 'อุณหภูมิร่างกายสูงสุด' COLLATE 'utf8_general_ci',
	`ap_bt_low` TEXT NULL DEFAULT NULL COMMENT 'อุณหภูมิร่างกายต่ำสุด' COLLATE 'utf8_general_ci',
	`bt_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน อุณหภูมิร่างกาย จาก APACHE III' COLLATE 'utf8_general_ci',
	`bt_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน อุณหภูมิร่างกาย จาก SAPS' COLLATE 'utf8_general_ci',
	`sap_sbp_high` TEXT NULL DEFAULT NULL COMMENT 'ความดันโลหิตซิสโตลิกสูงสุด' COLLATE 'utf8_general_ci',
	`sap_sbp_low` TEXT NULL DEFAULT NULL COMMENT 'ความดันโลหิตซิสโตลิกต่ำสุด' COLLATE 'utf8_general_ci',
	`sbp_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ความดันโลหิตซิสโตลิก จาก SAPS' COLLATE 'utf8_general_ci',
	`ap_map_high` TEXT NULL DEFAULT NULL COMMENT 'ความดันโลหิตเฉลี่ยสูงสุด' COLLATE 'utf8_general_ci',
	`ap_map_low` TEXT NULL DEFAULT NULL COMMENT 'ความดันโลหิตเฉลี่ยต่ำสุด' COLLATE 'utf8_general_ci',
	`map_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ความดันโลหิตเฉลี่ย จาก APACHE III' COLLATE 'utf8_general_ci',
	`sofa_vaso_ino` TEXT NULL DEFAULT NULL COMMENT 'คะแนน SOFA ของการใช้ยาหลอดเลือด' COLLATE 'utf8_general_ci',
	`sofa_vaso_choice___1` TEXT NULL DEFAULT NULL COMMENT 'ชนิดยาหลอดเลือดที่ 1' COLLATE 'utf8_general_ci',
	`sofa_vaso_choice___2` TEXT NULL DEFAULT NULL COMMENT 'ชนิดยาหลอดเลือดที่ 2' COLLATE 'utf8_general_ci',
	`sofa_vaso_choice___3` TEXT NULL DEFAULT NULL COMMENT 'ชนิดยาหลอดเลือดที่ 3' COLLATE 'utf8_general_ci',
	`sofa_vaso_choice___4` TEXT NULL DEFAULT NULL COMMENT 'ชนิดยาหลอดเลือดที่ 4' COLLATE 'utf8_general_ci',
	`sofa_vaso_choice___5` TEXT NULL DEFAULT NULL COMMENT 'ชนิดยาหลอดเลือดที่ 5' COLLATE 'utf8_general_ci',
	`sofa_vaso_choice___6` TEXT NULL DEFAULT NULL COMMENT 'ชนิดยาหลอดเลือดที่ 6' COLLATE 'utf8_general_ci',
	`sofa_vaso_choice___na` TEXT NULL DEFAULT NULL COMMENT 'ชนิดยาหลอดเลือดไม่ระบุ' COLLATE 'utf8_general_ci',
	`ap_dopa_conc` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นของยา Dopamine' COLLATE 'utf8_general_ci',
	`ap_dopa_conc_oth` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นยาหลอดเลือดชนิดอื่น' COLLATE 'utf8_general_ci',
	`ap_dopa_rate` TEXT NULL DEFAULT NULL COMMENT 'อัตราการใช้ Dopamine' COLLATE 'utf8_general_ci',
	`ap_dopa_dose` TEXT NULL DEFAULT NULL COMMENT 'ขนาดยา Dopamine' COLLATE 'utf8_general_ci',
	`sofa_dopa` TEXT NULL DEFAULT NULL COMMENT 'คะแนน SOFA ของการใช้ Dopamine' COLLATE 'utf8_general_ci',
	`ap_dobu_conc` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นของยา Dobutamine' COLLATE 'utf8_general_ci',
	`ap_dobu_conc_oth` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นยาหลอดเลือดชนิดอื่น' COLLATE 'utf8_general_ci',
	`ap_dobu_rate` TEXT NULL DEFAULT NULL COMMENT 'อัตราการใช้ Dobutamine' COLLATE 'utf8_general_ci',
	`ap_dobu_dose` TEXT NULL DEFAULT NULL COMMENT 'ขนาดยา Dobutamine' COLLATE 'utf8_general_ci',
	`sofa_dobu` TEXT NULL DEFAULT NULL COMMENT 'คะแนน SOFA ของการใช้ Dobutamine' COLLATE 'utf8_general_ci',
	`ap_epi_conc` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นของยา Epinephrine' COLLATE 'utf8_general_ci',
	`ap_epi_conc_oth` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นยาหลอดเลือดชนิดอื่น' COLLATE 'utf8_general_ci',
	`ap_epi_rate` TEXT NULL DEFAULT NULL COMMENT 'อัตราการใช้ Epinephrine' COLLATE 'utf8_general_ci',
	`ap_epi_dose` TEXT NULL DEFAULT NULL COMMENT 'ขนาดยา Epinephrine' COLLATE 'utf8_general_ci',
	`sofa_epi` TEXT NULL DEFAULT NULL COMMENT 'คะแนน SOFA ของการใช้ Epinephrine' COLLATE 'utf8_general_ci',
	`ap_norepi_conc` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นยาหลอดเลือดชนิดอื่น' COLLATE 'utf8_general_ci',
	`ap_norepi_conc_oth` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นยาหลอดเลือดชนิดอื่น' COLLATE 'utf8_general_ci',
	`ap_norepi_rate` TEXT NULL DEFAULT NULL COMMENT 'อัตราการใช้ Norepinephrine' COLLATE 'utf8_general_ci',
	`ap_norepi_dose` TEXT NULL DEFAULT NULL COMMENT 'ขนาดยา Norepinephrine' COLLATE 'utf8_general_ci',
	`sofa_norepi` TEXT NULL DEFAULT NULL COMMENT 'คะแนน SOFA ของการใช้ Norepinephrine' COLLATE 'utf8_general_ci',
	`ap_phenyl_conc` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นยาหลอดเลือดชนิดอื่น' COLLATE 'utf8_general_ci',
	`ap_phenyl_conc_oth` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นยาหลอดเลือดชนิดอื่น' COLLATE 'utf8_general_ci',
	`ap_phenyl_rate` TEXT NULL DEFAULT NULL COMMENT 'อัตราการใช้ Phenylephrine' COLLATE 'utf8_general_ci',
	`ap_phenyl_dose` TEXT NULL DEFAULT NULL COMMENT 'ขนาดยา Phenylephrine' COLLATE 'utf8_general_ci',
	`sofa_phenyl` TEXT NULL DEFAULT NULL COMMENT 'คะแนน SOFA ของการใช้ Phenylephrine' COLLATE 'utf8_general_ci',
	`ap_vasop_conc` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นยาหลอดเลือดชนิดอื่น' COLLATE 'utf8_general_ci',
	`ap_vasop_conc_oth` TEXT NULL DEFAULT NULL COMMENT 'ความเข้มข้นยาหลอดเลือดชนิดอื่น' COLLATE 'utf8_general_ci',
	`ap_vasop_rate` TEXT NULL DEFAULT NULL COMMENT 'อัตราการใช้ Vasopressor' COLLATE 'utf8_general_ci',
	`ap_vasop_dose` TEXT NULL DEFAULT NULL COMMENT 'ขนาดยา Vasopressor' COLLATE 'utf8_general_ci',
	`sofa_vasop` TEXT NULL DEFAULT NULL COMMENT 'คะแนน SOFA ของการใช้ Vasopressor' COLLATE 'utf8_general_ci',
	`cvs_point` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Cardiovascular จาก SOFA' COLLATE 'utf8_general_ci',
	`ap_hr_high` TEXT NULL DEFAULT NULL COMMENT 'อัตราการเต้นของหัวใจสูงสุด' COLLATE 'utf8_general_ci',
	`ap_hr_low` TEXT NULL DEFAULT NULL COMMENT 'อัตราการเต้นของหัวใจต่ำสุด' COLLATE 'utf8_general_ci',
	`hr_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน อัตราการเต้นของหัวใจ จาก APACHE III' COLLATE 'utf8_general_ci',
	`hr_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน อัตราการเต้นของหัวใจ จาก SAPS' COLLATE 'utf8_general_ci',
	`ap_rr_high` TEXT NULL DEFAULT NULL COMMENT 'อัตราการหายใจสูงสุด' COLLATE 'utf8_general_ci',
	`ap_rr_low` TEXT NULL DEFAULT NULL COMMENT 'อัตราการหายใจต่ำสุด' COLLATE 'utf8_general_ci',
	`rr_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน อัตราการหายใจ จาก APACHE III' COLLATE 'utf8_general_ci',
	`ap2_int` TEXT NULL DEFAULT NULL COMMENT 'ระยะเวลาระหว่างวัด' COLLATE 'utf8_general_ci',
	`sap_mv` TEXT NULL DEFAULT NULL COMMENT 'ใช้เครื่องช่วยหายใจ' COLLATE 'utf8_general_ci',
	`ap_abg` TEXT NULL DEFAULT NULL COMMENT 'จำนวนครั้งที่ตรวจ ABG' COLLATE 'utf8_general_ci',
	`sofa_spo2` TEXT NULL DEFAULT NULL COMMENT 'คะแนน SpO2 จาก SOFA' COLLATE 'utf8_general_ci',
	`sofa_fio2` TEXT NULL DEFAULT NULL COMMENT 'คะแนน FiO2 จาก SOFA' COLLATE 'utf8_general_ci',
	`sofa_pf_sf` TEXT NULL DEFAULT NULL COMMENT 'คะแนน PF/SF จาก SOFA' COLLATE 'utf8_general_ci',
	`ap_abg_num` TEXT NULL DEFAULT NULL COMMENT 'จำนวนครั้งที่ตรวจ ABG' COLLATE 'utf8_general_ci',
	`ap_ph_1` TEXT NULL DEFAULT NULL COMMENT 'ค่า pH จากการตรวจ ABG ครั้งที่ 1' COLLATE 'utf8_general_ci',
	`ap_paco2_1` TEXT NULL DEFAULT NULL COMMENT 'ค่า PaCO2 จากการตรวจ ABG ครั้งที่ 1' COLLATE 'utf8_general_ci',
	`ap_pao2_1` TEXT NULL DEFAULT NULL COMMENT 'ค่า PaO2 จากการตรวจ ABG ครั้งที่ 1' COLLATE 'utf8_general_ci',
	`ap_fio2_1` TEXT NULL DEFAULT NULL COMMENT 'ค่า FiO2 จากการตรวจ ABG ครั้งที่ 1' COLLATE 'utf8_general_ci',
	`ap_pf_1` TEXT NULL DEFAULT NULL COMMENT 'ค่า PF Ratio จากการตรวจ ABG ครั้งที่ 1' COLLATE 'utf8_general_ci',
	`ap_aado_1` TEXT NULL DEFAULT NULL COMMENT 'ค่า A-aDO2 จากการตรวจ ABG ครั้งที่ 1' COLLATE 'utf8_general_ci',
	`ap_oxygenation_point_1` TEXT NULL DEFAULT NULL COMMENT 'คะแนนด้านการขาดออกซิเจน จากการตรวจ ABG ครั้งที่ 1' COLLATE 'utf8_general_ci',
	`ap_acidbase_point_1` TEXT NULL DEFAULT NULL COMMENT 'คะแนนด้านสมดุลกรด-ด่าง จากการตรวจ ABG ครั้งที่ 1' COLLATE 'utf8_general_ci',
	`ap_ph_2` TEXT NULL DEFAULT NULL COMMENT 'ค่า pH จากการตรวจ ABG ครั้งที่ 2' COLLATE 'utf8_general_ci',
	`ap_paco2_2` TEXT NULL DEFAULT NULL COMMENT 'ค่า PaCO2 จากการตรวจ ABG ครั้งที่ 2' COLLATE 'utf8_general_ci',
	`ap_pao2_2` TEXT NULL DEFAULT NULL COMMENT 'ค่า PaO2 จากการตรวจ ABG ครั้งที่ 2' COLLATE 'utf8_general_ci',
	`ap_fio2_2` TEXT NULL DEFAULT NULL COMMENT 'ค่า FiO2 จากการตรวจ ABG ครั้งที่ 2' COLLATE 'utf8_general_ci',
	`ap_pf_2` TEXT NULL DEFAULT NULL COMMENT 'ค่า PF Ratio จากการตรวจ ABG ครั้งที่ 2' COLLATE 'utf8_general_ci',
	`ap_aado_2` TEXT NULL DEFAULT NULL COMMENT 'ค่า A-aDO2 จากการตรวจ ABG ครั้งที่ 2' COLLATE 'utf8_general_ci',
	`ap_oxygenation_point_2` TEXT NULL DEFAULT NULL COMMENT 'คะแนนด้านการขาดออกซิเจน จากการตรวจ ABG ครั้งที่ 2' COLLATE 'utf8_general_ci',
	`ap_acidbase_point_2` TEXT NULL DEFAULT NULL COMMENT 'คะแนนด้านสมดุลกรด-ด่าง จากการตรวจ ABG ครั้งที่ 2' COLLATE 'utf8_general_ci',
	`ap_ph_3` TEXT NULL DEFAULT NULL COMMENT 'ค่า pH จากการตรวจ ABG ครั้งที่ 3' COLLATE 'utf8_general_ci',
	`ap_paco2_3` TEXT NULL DEFAULT NULL COMMENT 'า PaCO2 จากการตรวจ ABG ครั้งที่ 3' COLLATE 'utf8_general_ci',
	`ap_pao2_3` TEXT NULL DEFAULT NULL COMMENT 'ค่า PaO2 จากการตรวจ ABG ครั้งที่ 3' COLLATE 'utf8_general_ci',
	`ap_fio2_3` TEXT NULL DEFAULT NULL COMMENT 'ค่า FiO2 จากการตรวจ ABG ครั้งที่ 3' COLLATE 'utf8_general_ci',
	`ap_pf_3` TEXT NULL DEFAULT NULL COMMENT 'ค่า PF Ratio จากการตรวจ ABG ครั้งที่ 3' COLLATE 'utf8_general_ci',
	`ap_aado_3` TEXT NULL DEFAULT NULL COMMENT 'ค่า A-aDO2 จากการตรวจ ABG ครั้งที่ 3' COLLATE 'utf8_general_ci',
	`ap_oxygenation_point_3` TEXT NULL DEFAULT NULL COMMENT 'คะแนนด้านการขาดออกซิเจน จากการตรวจ ABG ครั้งที่ 3' COLLATE 'utf8_general_ci',
	`ap_acidbase_point_3` TEXT NULL DEFAULT NULL COMMENT 'คะแนนด้านสมดุลกรด-ด่าง จากการตรวจ ABG ครั้งที่ 3' COLLATE 'utf8_general_ci',
	`ap_ph_4` TEXT NULL DEFAULT NULL COMMENT 'ค่า pH จากการตรวจ ABG ครั้งที่ 4' COLLATE 'utf8_general_ci',
	`ap_paco2_4` TEXT NULL DEFAULT NULL COMMENT 'ค่า PaCO2 จากการตรวจ ABG ครั้งที่ 4' COLLATE 'utf8_general_ci',
	`ap_pao2_4` TEXT NULL DEFAULT NULL COMMENT 'ค่า PaO2 จากการตรวจ ABG ครั้งที่ 4' COLLATE 'utf8_general_ci',
	`ap_fio2_4` TEXT NULL DEFAULT NULL COMMENT 'ค่า FiO2 จากการตรวจ ABG ครั้งที่ 4' COLLATE 'utf8_general_ci',
	`ap_pf_4` TEXT NULL DEFAULT NULL COMMENT 'ค่า PF Ratio จากการตรวจ ABG ครั้งที่ 4' COLLATE 'utf8_general_ci',
	`ap_aado_4` TEXT NULL DEFAULT NULL COMMENT 'ค่า A-aDO2 จากการตรวจ ABG ครั้งที่ 4' COLLATE 'utf8_general_ci',
	`ap_oxygenation_point_4` TEXT NULL DEFAULT NULL COMMENT 'คะแนนด้านการขาดออกซิเจน จากการตรวจ ABG ครั้งที่ 4' COLLATE 'utf8_general_ci',
	`ap_acidbase_point_4` TEXT NULL DEFAULT NULL COMMENT 'คะแนนด้านสมดุลกรด-ด่าง จากการตรวจ ABG ครั้งที่ 4' COLLATE 'utf8_general_ci',
	`oxygenation_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนนด้านการขาดออกซิเจน จาก APACHE III' COLLATE 'utf8_general_ci',
	`acid_base_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนนด้านสมดุลกรด-ด่าง จาก APACHE III' COLLATE 'utf8_general_ci',
	`rs_point` TEXT NULL DEFAULT NULL COMMENT 'คะแนนระบบการหายใจ จาก SOFA' COLLATE 'utf8_general_ci',
	`pf_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน PF Ratio จาก SAPS' COLLATE 'utf8_general_ci',
	`ap_glu_high` TEXT NULL DEFAULT NULL COMMENT 'ระดับน้ำตาลในเลือดสูงสุด' COLLATE 'utf8_general_ci',
	`ap_glu_low` TEXT NULL DEFAULT NULL COMMENT 'ระดับน้ำตาลในเลือดต่ำสุด' COLLATE 'utf8_general_ci',
	`glu_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ระดับน้ำตาลในเลือด จาก APACHE III' COLLATE 'utf8_general_ci',
	`ap_bun` TEXT NULL DEFAULT NULL COMMENT 'ระดับ BUN' COLLATE 'utf8_general_ci',
	`bun_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ระดับ BUN จาก APACHE III' COLLATE 'utf8_general_ci',
	`bun_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ระดับ BUN จาก SAPS' COLLATE 'utf8_general_ci',
	`ap_cr_high` TEXT NULL DEFAULT NULL COMMENT 'ระดับครีเอทินินสูงสุด' COLLATE 'utf8_general_ci',
	`ap_cr_low` TEXT NULL DEFAULT NULL COMMENT 'ระดับครีเอทินินต่ำสุด' COLLATE 'utf8_general_ci',
	`ap_na_high` TEXT NULL DEFAULT NULL COMMENT 'ระดับโซเดียมสูงสุด' COLLATE 'utf8_general_ci',
	`ap_na_low` TEXT NULL DEFAULT NULL COMMENT 'ระดับโซเดียมต่ำสุด' COLLATE 'utf8_general_ci',
	`sodium_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ระดับโซเดียม จาก APACHE III' COLLATE 'utf8_general_ci',
	`sodium_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ระดับโซเดียม จาก SAPS' COLLATE 'utf8_general_ci',
	`ap2_k_high` TEXT NULL DEFAULT NULL COMMENT 'ระดับโพแทสเซียมสูงสุด' COLLATE 'utf8_general_ci',
	`ap2_k_low` TEXT NULL DEFAULT NULL COMMENT 'ระดับโพแทสเซียมต่ำสุด' COLLATE 'utf8_general_ci',
	`potassium_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ระดับโพแทสเซียม จาก SAPS' COLLATE 'utf8_general_ci',
	`ap2_hco3` TEXT NULL DEFAULT NULL COMMENT 'ระดับ HCO3' COLLATE 'utf8_general_ci',
	`hco3_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ระดับ HCO3 จาก SAPS' COLLATE 'utf8_general_ci',
	`lactate_24h` TEXT NULL DEFAULT NULL COMMENT 'ระดับ Lactate ในระยะเวลา 24 ชั่วโมง' COLLATE 'utf8_general_ci',
	`lact_unit` TEXT NULL DEFAULT NULL COMMENT 'หน่วยวัดระดับ Lactate' COLLATE 'utf8_general_ci',
	`ap_uo` TEXT NULL DEFAULT NULL COMMENT 'ปริมาณปัสสาวะ' COLLATE 'utf8_general_ci',
	`uo_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ปริมาณปัสสาวะ จาก APACHE III' COLLATE 'utf8_general_ci',
	`renal_point` TEXT NULL DEFAULT NULL COMMENT 'คะแนนระบบไต จาก SOFA' COLLATE 'utf8_general_ci',
	`ap_arf` TEXT NULL DEFAULT NULL COMMENT 'มีภาวะไตวายเฉียบพลัน' COLLATE 'utf8_general_ci',
	`renal_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนนระบบไต จาก APACHE III' COLLATE 'utf8_general_ci',
	`uo_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ปริมาณปัสสาวะ จาก SAPS' COLLATE 'utf8_general_ci',
	`ap_alb_high` TEXT NULL DEFAULT NULL COMMENT 'ระดับ Albumin สูงสุด' COLLATE 'utf8_general_ci',
	`ap_alb_low` TEXT NULL DEFAULT NULL COMMENT 'ระดับ Albumin ต่ำสุด' COLLATE 'utf8_general_ci',
	`alb_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ระดับ Albumin จาก APACHE III' COLLATE 'utf8_general_ci',
	`ap_tb` TEXT NULL DEFAULT NULL COMMENT 'ระดับ Total Bilirubin' COLLATE 'utf8_general_ci',
	`tb_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ระดับ Total Bilirubin จาก APACHE III' COLLATE 'utf8_general_ci',
	`liver_point` TEXT NULL DEFAULT NULL COMMENT 'คะแนนระบบตับ จาก SOFA' COLLATE 'utf8_general_ci',
	`tb_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ระดับ Total Bilirubin จาก SAPS' COLLATE 'utf8_general_ci',
	`ap_wbc_high` TEXT NULL DEFAULT NULL COMMENT 'จำนวน White Blood Cells สูงสุด' COLLATE 'utf8_general_ci',
	`ap_wbc_low` TEXT NULL DEFAULT NULL COMMENT 'จำนวน White Blood Cells ต่ำสุด' COLLATE 'utf8_general_ci',
	`wbc_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน จำนวน White Blood Cells จาก APACHE III' COLLATE 'utf8_general_ci',
	`wbc_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน จำนวน White Blood Cells จาก SAPS' COLLATE 'utf8_general_ci',
	`ap_hct_high` TEXT NULL DEFAULT NULL COMMENT 'ค่า Hematocrit สูงสุด' COLLATE 'utf8_general_ci',
	`ap_hct_low` TEXT NULL DEFAULT NULL COMMENT 'ค่า Hematocrit ต่ำสุด' COLLATE 'utf8_general_ci',
	`hct_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน ค่า Hematocrit จาก APACHE III' COLLATE 'utf8_general_ci',
	`sofa_plt` TEXT NULL DEFAULT NULL COMMENT 'คะแนนระบบการแข็งตัวของเลือด จาก SOFA' COLLATE 'utf8_general_ci',
	`coag_point` TEXT NULL DEFAULT NULL COMMENT 'คะแนนระบบการแข็งตัวของเลือด จาก SOFA' COLLATE 'utf8_general_ci',
	`ap2_gcs_e` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Eye Response ใน GCS' COLLATE 'utf8_general_ci',
	`ap2_gcs_v` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Verbal Response ใน GCS' COLLATE 'utf8_general_ci',
	`ap2_gcs_m` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Motor Response ใน GCS' COLLATE 'utf8_general_ci',
	`ap2_gcs_v_predict` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Verbal Response ที่คาดการณ์ใน GCS' COLLATE 'utf8_general_ci',
	`ap2_gcs` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Glasgow Coma Scale โดยรวม' COLLATE 'utf8_general_ci',
	`neuro_point_ap3` TEXT NULL DEFAULT NULL COMMENT 'คะแนนระบบประสาทส่วนกลาง จาก APACHE III' COLLATE 'utf8_general_ci',
	`cns_point` TEXT NULL DEFAULT NULL COMMENT 'คะแนนระบบประสาทส่วนกลาง จาก SOFA' COLLATE 'utf8_general_ci',
	`gcs_point_sap` TEXT NULL DEFAULT NULL COMMENT 'คะแนน Glasgow Coma Scale จาก SAPS' COLLATE 'utf8_general_ci',
	`apache_3` TEXT NULL DEFAULT NULL COMMENT 'คะแนน APACHE III' COLLATE 'utf8_general_ci',
	`sofa` TEXT NULL DEFAULT NULL COMMENT 'คะแนน SOFA' COLLATE 'utf8_general_ci',
	`sap_2` TEXT NULL DEFAULT NULL COMMENT 'คะแนน SAPS II' COLLATE 'utf8_general_ci',
	`apache_iii_sofa_and_sap_ii_score_complete` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`sdx` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`secondary_diagnosis_complete` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mv_icu` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`num_ettube` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mv_start_1` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mv_end_1` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mv_start_2` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mv_end_2` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mv_start_3` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mv_end_3` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mv_start_4` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mv_end_4` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mv_duration` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`niv` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`ecmo` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`iabp` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`hemoperfusion` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`shock` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`shock_type___1` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`shock_type___2` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`shock_type___3` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`shock_type___4` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`shock_type___5` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`shock_type___na` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`ino_vaso` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`vaso_start_date` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`vaso_stop_date` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`shock_reversal_time` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`ttm` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`plex` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`pan_inf` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`thrombolytic_ami` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`frailty` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`delirium` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`press_inj` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`press_inj_stage` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cumm_fluid` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`fluid_overload` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`data_complete` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`date` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`day` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`scr_daily` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`intake_daily` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`output_daily` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`uo_daily` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`io_daily` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`cum_fluid_daily` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`fluid_overload_daily` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`daily_scr_and_fluid_balance_complete` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_onset` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_stage` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_date` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`time_icutoaki` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___1` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___2` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___3` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___4` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___5` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___6` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___7` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___8` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___9` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___10` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___11` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___12` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_etio___na` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`fst` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`fst_response` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`diuretic_bf` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___1` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___2` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___3` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___4` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___5` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___6` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___7` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___8` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___9` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___10` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___0` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_bf___na` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___1` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___2` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___3` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___4` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___5` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___6` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___7` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___8` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___9` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___10` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___0` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`nephrolist_af___na` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`rrt` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`rrt_start` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`rrt_type___1` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`rrt_type___2` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`rrt_type___3` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`rrt_type___4` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`rrt_type___na` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`aki_charateristics_complete` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`day_28_rrt` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`day_28_aki` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`day_90_aki` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`free_rrt` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`rrt_free_date` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`rrt_duration` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`rfd_28` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`scr_28` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`scr_90` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`recovery5gr` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`renal_recover_date` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`fu_date` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`recovery_time` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mort_28_aki` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`last_date_28_aki` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`surv_28_aki` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mort_90_aki` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`last_date_90_aki` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`surv_90_aki` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`renal_recovery_and_outcome_complete` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`day_28` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`day_90` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`icu_dc_decide_date` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`icu_dc_date` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`icu_dc_dest` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`icu_los` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`h_dc_date` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`h_dc_dest` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`h_los` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`h_mort` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`icu_mort` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mort_28` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`last_date_28` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`surv_28` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`mort_90` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`last_date_90` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`surv_90` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`vfd_28` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`sfd_28` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`outcomes_complete` TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	PRIMARY KEY (`id`) USING BTREE,
	INDEX `hn` (`hn`) USING BTREE,
	INDEX `postcode` (`postcode`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
"""

def get_env(key: str, default: str = "") -> str:
    """Get environment variable"""
    return os.getenv(key, default)


# REDCap Configuration (from environment variables)
REDCAP_API_URL = get_env('REDCAP_API_URL', '')
REDCAP_API_TOKEN = get_env('REDCAP_API_TOKEN', '')


def get_redcap_data() -> tuple:
    """
    ดึงข้อมูลจาก REDCap API
    
    Returns:
        tuple: (records, field_names)
    """
    try:
        logger.info("🔗 Connecting to REDCap API...")
        project = Project(REDCAP_API_URL, REDCAP_API_TOKEN)
        
        # Export all records
        logger.info("📥 Exporting records from REDCap...")
        records = project.export_records(format_type='json')
        
        if not records:
            logger.warning("⚠️  No records found in REDCap project")
            return [], []
        
        # Get field names from first record
        field_names = list(records[0].keys()) if records else []
        
        logger.info(f"✅ Exported {len(records)} records with {len(field_names)} fields")
        logger.info(f"📝 Fields: {', '.join(field_names[:10])}{'...' if len(field_names) > 10 else ''}")
        
        return records, field_names
        
    except Exception as e:
        logger.error(f"❌ Error fetching REDCap data: {e}")
        raise


def create_redcap_table(cursor, table_name: str, field_names: List[str]):
    """
    สร้างตารางสำหรับข้อมูล REDCap (ใช้ pre-defined schema ที่ไม่มี row size error)
    
    Args:
        cursor: Database cursor
        table_name: ชื่อตาราง
        field_names: รายชื่อฟิลด์ (ไม่ใช้ เพราะใช้ pre-defined schema)
    """
    try:
        logger.info(f"🔨 Creating table '{table_name}' with pre-defined schema (TEXT columns)...")
        
        # Drop existing table
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        
        # Use pre-defined CREATE TABLE statement
        # This uses TEXT for most fields and VARCHAR(255) for key fields
        # No row size error because TEXT stores off-page
        create_stmt = CREATE_TABLE_SQL.format(table_name=table_name)
        
        cursor.execute(create_stmt)
        logger.info(f"✅ Table '{table_name}' created successfully (395 columns)")
        
    except Exception as e:
        logger.error(f"❌ Error creating table '{table_name}': {e}")
        raise


def insert_redcap_data(cursor, table_name: str, records: List[Dict[str, Any]], field_names: List[str]):
    """
    โหลดข้อมูล REDCap ลงตาราง (insert เฉพาะ fields ที่มีในตาราง)
    
    Args:
        cursor: Database cursor
        table_name: ชื่อตาราง
        records: รายการข้อมูล
        field_names: รายชื่อฟิลด์จาก REDCap
    """
    try:
        if not records:
            logger.warning("⚠️  No records to insert")
            return
        
        logger.info(f"📥 Preparing to insert {len(records)} records into '{table_name}'...")
        
        # Get actual columns from table (exclude auto-generated columns)
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
        table_columns = [row[0] for row in cursor.fetchall()]
        
        # Exclude id and loaded_at (auto-generated)
        table_columns = [col for col in table_columns if col not in ['id', 'loaded_at']]
        
        # Find matching fields between REDCap data and table columns
        matching_fields = [f for f in field_names if f in table_columns]
        missing_fields = [f for f in field_names if f not in table_columns]
        
        if missing_fields:
            logger.warning(f"⚠️  {len(missing_fields)} fields from REDCap not in table schema (will skip):")
            logger.warning(f"   {', '.join(missing_fields[:5])}{'...' if len(missing_fields) > 5 else ''}")
        
        logger.info(f"   📊 Using {len(matching_fields)}/{len(field_names)} fields")
        
        # Build INSERT statement with matching fields only
        placeholders = ', '.join(['%s'] * len(matching_fields))
        columns = ', '.join([f"`{f}`" for f in matching_fields])
        insert_stmt = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        
        # Prepare data (only matching fields)
        insert_data = []
        for record in records:
            row = []
            for field in matching_fields:
                value = record.get(field, None)
                if value is not None:
                    row.append(str(value))
                else:
                    row.append(None)
            insert_data.append(tuple(row))
        
        # Batch insert
        batch_size = 1000
        total_inserted = 0
        
        logger.info(f"   💾 Inserting data in batches...")
        for i in range(0, len(insert_data), batch_size):
            batch = insert_data[i:i + batch_size]
            cursor.executemany(insert_stmt, batch)
            total_inserted += len(batch)
            if (i + batch_size) % 5000 == 0:  # Log every 5K
                logger.info(f"      Inserted {total_inserted:,}/{len(records):,} records...")
        
        logger.info(f"✅ Successfully inserted {total_inserted:,} records")
        
    except Exception as e:
        logger.error(f"❌ Error inserting data into '{table_name}': {e}")
        raise


async def load_redcap_to_database(table_name: str = "redcap_data"):
    """
    Main function: ดึงข้อมูลจาก REDCap และโหลดลงฐานข้อมูล
    
    Args:
        table_name: ชื่อตารางที่จะสร้าง (default: redcap_data)
    
    Returns:
        bool: True ถ้าสำเร็จ, False ถ้าล้มเหลว
    """
    try:
        logger.info("="*80)
        logger.info("🚀 Starting REDCap Data Loader")
        logger.info("="*80)
        
        # 1. Fetch data from REDCap
        loop = asyncio.get_event_loop()
        records, field_names = await loop.run_in_executor(None, get_redcap_data)
        
        if not records:
            logger.warning("⚠️  No data to load")
            return False
        
        # 2. Get destination database connection
        dst_host = get_env('DST_DB_HOST', 'localhost')
        dst_port = int(get_env('DST_DB_PORT', '3306'))
        dst_user = get_env('DST_DB_USER', 'root')
        dst_password = get_env('DST_DB_PASSWORD', '')
        
        # Check if dynamic mode is enabled
        is_dynamic = get_env('DST_DB_DYNAMIC', 'false').lower() == 'true'
        
        if is_dynamic:
            # Use dynamic database name
            from main import get_dynamic_db_name
            dst_db = get_dynamic_db_name()
            logger.info(f"🗓️  Using dynamic database: {dst_db}")
        else:
            dst_db = get_env('DST_DB_NAME', 'dst_db')
            logger.info(f"📂 Using static database: {dst_db}")
        
        # 3. Connect to database (without selecting database first)
        conn_params = {
            'host': dst_host,
            'port': dst_port,
            'user': dst_user,
            'password': dst_password,
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci'
        }
        
        logger.info(f"🔗 Connecting to destination database at {dst_host}:{dst_port}...")
        conn = mysql.connector.connect(**conn_params)
        cursor = conn.cursor()
        
        # 4. Ensure database exists (for dynamic mode)
        if is_dynamic:
            logger.info(f"🔨 Creating database '{dst_db}' if not exists...")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{dst_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        
        # 5. Use database
        cursor.execute(f"USE `{dst_db}`")
        logger.info(f"✅ Using database: {dst_db}")
        
        # 6. Create table
        create_redcap_table(cursor, table_name, field_names)
        
        # 7. Insert data
        insert_redcap_data(cursor, table_name, records, field_names)
        
        # 8. Commit and close
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("="*80)
        logger.info(f"✅ REDCap data loaded successfully to table '{table_name}'")
        logger.info(f"   📊 Records: {len(records)}")
        logger.info(f"   📝 Fields: {len(field_names)}")
        logger.info(f"   🗄️  Database: {dst_db}")
        logger.info("="*80)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ REDCap loader failed: {e}")
        return False


if __name__ == "__main__":
    # For testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    
    asyncio.run(load_redcap_to_database())
