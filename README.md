# IGIS Spatial Data Analytics(SDA)
<div align="center">
 📆 2024.03 ~ Ongoing
</div>

## Data Science Process(DSP)
![dspd](/asset/pngs/dspdesign.png)

## SDA Framework
![sdaf](/asset/pngs/sdaframework.png)

![sdad](/asset/pngs/sdadesign.png)

## Research Docs
**BD Scoring**: https://www.notion.so/d-lab-igis/Scoring-c9c9dd29fdab48709ff7bfe285f93712?pvs=4

## Experiment Execution

SDA 프로세스에 따라 아래 작업을 수행한다.

1. **brtgeo**: 건축물대장 데이터(bddb.brtitle)와 공간데이터(geodb.sd), 공간데이터지오(geodb.sdgeo)를 연결한다. pnu를 기준으로 연결하며, 이 과정에서 pnu가 속해있는 기초구역(basec)도 맵핑을 한다.

- input: `bddb.brtitle`, `geodb.sd`, `geodb.sdgeo`
- output: `brtgeo`(brtitle with geometry)

```bash
cd code
/opt/anaconda3/envs/dasda/bin/python brtgeo.py -e=local -d=20240401
```

2. **pnugeo**: 지번주소 데이터(bddb.jibunaddress)와 공간데이터(geodb.sd), 공간데이터지오(geodb.sdgeo)를 연결한다. pnu를 기준으로 연결하며, 이 과정에서 pnu가 속해있는 기초구역(basec)도 맵핑을 한다.

- input: `bddb.jibunaddress`, `geodb.sd`, `geodb.sdgeo`
- output: `pnugeo`(pnu with geometry)

```bash
cd code
/opt/anaconda3/envs/dasda/bin/python pnugeo.py -e=local -d=20240401
```

3. **pnugeoxyv**: **pnugeo**에서 추출된 데이터를 Base로 하여, feature db에 있는 버스, 지하철, 기업재무 데이터를 mapping 한다. distance decay로 해당 지번에 대한 각 feature들 value로 맵핑한다. 서울특별시 지번의 갯수가 대략 90만개다. base point가 90만개이고 각 feature에 대해 거리를 산출하고 관련된 value를 반영하는 작업은 상당히 오래 걸린다. 1개년도 bus feature반영하는데 대략 5시간이 소요된다.

_`featuredb.salary` 데이터는 반영되지 않음. salary 데이터의 최소 공간데이터 단위는 기초구역이다. 기초구역보다 작은 공간데이터 단위(지번, 빌딩)로 salary 값을 얻을 수 없기 때문이다. 대신 brtgeoxyv 단계에서 기초구역번호를 근거로 데이터를 할당한다_


- input: `pnugeo`, `featuredb.metro`, `featuredb.bus`, `featuredb.corpfin`
- output: `pnugeoxyv` (pnu with feature nominal values with geometry)

```bash
cd code
/opt/anaconda3/envs/dasda/bin/python pnugeoxyv.py -e=local -d=20240401
```

4. **brtgeoxyv**: **brtgeo**의 빌딩은 모두 pnu를 가지고 있다. 토지위에 있지 않는 건물은 없기 때문이다. **pnugeoxyv**의 pnu를 기준으로 **brtgeo**에 해당하는 xyv를 가지고 온다. 만약 타임시리즈데이터를 누적시켜 저장하고 싶다면 -ts=True argument를 추가하면 된다.

- input: `brtgeo`, `pnugeoxyv`, `featuredb.salary`
- output: `brtgeoxyv`(brtitle with feature nominal values with geometry)

```bash
cd code
/opt/anaconda3/envs/dasda/bin/python brtgeoxyv.py -e=local -d=20240401
# or
/opt/anaconda3/envs/dasda/bin/python brtgeoxyv.py -e=local -d=20240401 -ts # if you want to create timeseries cumulated data
```

5. **bdscoring**:  **brtgeoxyv**를 바탕으로 feature 들에 따라 business district scoring을 수행한다. 이를 위해 각 feature의 nominal value를 정규화(standardize)한다. 이후 아래 Equation과 같이 Bd Scoring을 수행한다. Score값은 각 feature들을 주어진 가중치(beta, $\beta$)에 따라 곱한 결과값을 Rank 함수에 투입했을 때의 반환값이다.

$$
S_{i} = P(\prod_{f \isin F} \beta_f* M(v_{if})) \\
\text{where} \\
\begin{align*}
S_{i} &: \text{score of base point }(i) \\
f &: \text{feature(e.g. metro od, bus od)} \\
F &: \text{the set of features} \\
\beta_f &: \text{beta value of feature}(f) \\
v_{if} &: \text{the value of feature}(f) \text{ at base point} (i) \\
M(\cdot) &: \text{min max scaled value(in percentage)} \\
P(\cdot) &: \text{percentile - rank function(the higher the value, the higher the rank)} 
\end{align*}
$$

```bash
cd code
/opt/anaconda3/envs/dasda/bin/python bdscoring.py -e=local -d=20240401
# or
/opt/anaconda3/envs/dasda/bin/python bdscoring.py -e=local -d=20240401 -l # if you want to load data to db
```



## Features
### Salary(국민연금납입액을 통해 추정한 급여)

~ 2024.07 적용분
기준소득월액
- 상한액: 590만원
- 하한액: 37만원

2024.07 적용분 ~
기준소득월액
- 상한액: 617만원 
- 하한액: 39만원

2023대비 2024 기초연금액: 3.6% 상승

- source
  - https://www.hani.co.kr/arti/society/rights/1123610.html


## References
- gdf.explore tile에 관해: https://wooiljeong.github.io/python/folium_tiles/

## Bibilography

1. Estimation of distance-decay parameters -GIS-based indicators of recreational accessibility
@inproceedings{skov2001estimation,
  title={Estimation of distance-decay parameters: GIS-based indicators of recreational accessibility.},
  author={Skov-Petersen, Hans},
  booktitle={ScanGIS},
  pages={237--258},
  year={2001}
}

https://www.researchgate.net/profile/Hans-Skov-Petersen/publication/228698631_Estimation_of_Distance_Decay_Parameters_-_GIS_Based_Indicators_of_Recreational_Accessibility/links/5bb65f9092851c7fde2e8916/Estimation-of-Distance-Decay-Parameters-GIS-Based-Indicators-of-Recreational-Accessibility.pdf

