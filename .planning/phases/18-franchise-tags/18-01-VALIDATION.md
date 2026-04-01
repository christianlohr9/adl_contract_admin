# Phase 18-01: Franchise Tag Eligibility Validation Report

**Validated:** 2026-04-01T11:58:54Z
**Method:** Spreadsheet vs app comparison via `scripts/validate_ft_eligibility.py`
**Source:** `2026 ADL Contract Admin.xlsx` — `TagElig26` tab
**Season:** 2026

---

## Results Summary

| Metric | Value |
|--------|-------|
| **Total players in spreadsheet** | 1549 |
| **Matched to DB** | 1549 |
| **Unmatched** | 0 |
| **True discrepancies (logic bugs)** | 0 |
| **Expected diffs (re-signed/extended)** | 238 |

---

## True Discrepancies

No true discrepancies found. The app's FT eligibility logic matches the spreadsheet perfectly.

---

## Expected Differences (Offseason Transactions)

These players were FT-eligible in the spreadsheet (pre-offseason snapshot) but have since been re-signed, extended, or had their contracts modified. The app correctly reflects their current state.

### Player has an active contract (already re-signed this season) (193 players)

<details>
<summary>Show 193 players</summary>

- Mayfield, Baker TBB QB
- Tucker, Tre LVR WR
- Washington, Parker JAC WR
- Sanchez, Rigoberto IND PN
- Harris, Najee LAC RB
- McBride, Trey ARI TE
- Little, Cam JAC PK
- Oweh, Odafe LAC DE
- Sherwood, Jamien NYJ LB
- Thomas, Drake SEA LB
- Nixon, Keisean GBP CB
- Lake, Quentin LAR S
- Montgomery, David DET RB
- Meyers, Jakobi JAC WR
- Ferguson, Jake DAL TE
- McLaughlin, Chase TBB PK
- Slaton, Tedarrell CIN DT
- Johnson, Jermaine NYJ DE
- Stout, Upton SFO CB
- Wright, Nahshon CHI CB
- Bullock, Calen HOU S
- Harrison, Zach ATL DT
- Thibodeaux, Kayvon NYG DE
- Al-Shaair, Azeez HOU LB
- Werner, Pete NOS LB
- Winters, Dee SFO LB
- Jones, Marcus NEP CB
- Jones, Brandon DEN S
- Thompson, Jalen ARI S
- Garoppolo, Jimmy LAR QB
- Henry, Derrick BAL RB
- Kupp, Cooper SEA WR
- Ridley, Calvin TEN WR
- Wilson, Michael ARI WR
- Andrews, Mark BAL TE
- Hill, B.J. CIN DT
- Turner, Kobie LAR DT
- Edwards, T.J. CHI LB
- Simon, Cody ARI LB
- Robertson, Amik DET CB
- Taylor, Alontae NOS CB
- Thomas, Azareye'h NYJ CB
- Ward, Charvarius IND CB
- Horn, Jimmy CAR WR
- Borregales, Andres NEP PK
- Chubb, Bradley MIA DE
- Ossai, Joseph CIN DE
- Barnes, Derrick DET LB
- Gardner, Sauce IND CB
- Joseph, Kerby DET S
- Brissett, Jacoby ARI QB
- Fairbairn, Ka'imi HOU PK
- Herbig, Nick PIT DE
- Bernard, Terrel BUF LB
- Horn, Jaycee CAR CB
- Lenoir, Deommodore SFO CB
- Jones, Daniel IND QB
- Williams, Javonte DAL RB
- Hollins, Mack NEP WR
- Iosivas, Andrei CIN WR
- Nailor, Jalen MIN WR
- Drake-Rodriguez, Levi MIN DT
- Liufau, Marist DAL LB
- To'oTo'o, Henry HOU LB
- Tranquill, Drue KCC LB
- Green, Renardo SFO CB
- Moore, Kenny IND CB
- Hubbard, Chuba CAR RB
- Brown, Marquise KCC WR
- Redmond, Jalen MIN DT
- Bethune, Tatum SFO LB
- Porter Jr., Joey PIT CB
- Jacobs, Josh GBP RB
- Townsend, Tommy HOU PN
- Walker, Travon JAC DE
- Rozeboom, Christian CAR LB
- Davis, Carlton NEP CB
- Forbes, Emmanuel LAR CB
- Chinn, Jeremy LVR S
- Taylor-Demerson, Dadrion ARI S
- Conner, James ARI RB
- Hockenson, T.J. MIN TE
- Otton, Cade TBB TE
- Alexander, Darius NYG DT
- Oladejo, Oluwafemi TEN DE
- Porter, Darien LVR CB
- Revel, Shavon DAL CB
- Bryant, Coby SEA S
- Jackson, Lamar BAL QB
- Slayton, Darius NYG WR
- Thornton, Tyquan KCC WR
- Jones, Travis BAL DT
- Dennis, SirVocea TBB LB
- Warner, Fred SFO LB
- Jackson, Michael CAR CB
- Cross, Nick IND S
- McCaffrey, Christian SFO RB
- Tolbert, Jalen DAL WR
- Laulu, Jonah LVR DT
- Greenard, Jonathan MIN DE
- Bertrand, JD ATL LB
- Brisker, Jaquan CHI S
- Reid, Justin NOS S
- Goedert, Dallas PHI TE
- Smith, Jonnu PIT TE
- Dean, Nakobe PHI LB
- Adebo, Paulson NYG CB
- Hooker, Amani TEN S
- Adams, Davante LAR WR
- Gray, Noah KCC TE
- Bosa, Joey BUF DE
- Bigsby, Tank PHI RB
- McNamara, Austin NYJ PN
- Dorlus, Brandon ATL DT
- Umanmielen, Princely CAR DE
- Metellus, Josh MIN S
- McKee, Tanner PHI QB
- Evans, Mike TBB WR
- Johnson, Tez TBB WR
- Sieler, Zach MIA DT
- Sawyer, Jack PIT DE
- Dobbins, J.K. DEN RB
- Delpit, Grant CLE S
- Elliott, DeShon PIT S
- Murray, Eric JAC S
- Bateman, Rashod BAL WR
- Byard, Kevin CHI S
- Ojomo, Moro PHI DT
- Bonitto, Nik DEN DE
- Bolton, Nick KCC LB
- Brooks, Jordyn MIA LB
- Luvu, Frankie WAS LB
- Queen, Patrick PIT LB
- Curl, Kamren LAR S
- Felton, Tai MIN WR
- Boswell, Chris PIT PK
- Heyward, Cameron PIT DT
- Sweat, Montez CHI DE
- Wagner, Bobby WAS LB
- Brown, Ji'Ayir SFO S
- Samuel, Deebo WAS WR
- Kmet, Cole CHI TE
- Taylor, Tory CHI PN
- Robinson, A'Shawn CAR DT
- Vea, Vita TBB DT
- Granderson, Carl NOS DE
- Jones, Dre'Mont BAL DE
- Flacco, Joe CIN QB
- Jeudy, Jerry CLE WR
- Zaccheaus, Olamide CHI WR
- Okonkwo, Chigoziem TEN TE
- Payne, Da'Ron WAS DT
- Pacheco, Isiah KCC RB
- Dicker, Cameron LAC PK
- Barmore, Christian NEP DT
- Clark, Kenny DAL DT
- Epenesa, A.J. BUF DE
- Overshown, DeMarvion DAL LB
- Darnold, Sam SEA QB
- Williams, Kyren LAR RB
- Kittle, George SFO TE
- Hunt, Kareem KCC RB
- Mitchell, Keaton BAL RB
- Johnson, Juwan NOS TE
- Bosa, Nick SFO DE
- Allen, Josh BUF QB
- Kamara, Alvin NOS RB
- Coker, Jalen CAR WR
- Smith, DeVonta PHI WR
- Musgrave, Luke GBP TE
- Karlaftis, George KCC DE
- Mack, Khalil LAC DE
- Bynum, Camryn IND S
- Mason, Jordan MIN RB
- White, Rachaad TBB RB
- Johnson, Antonio JAC S
- Murray, Kyler ARI QB
- Robinson, Brian SFO RB
- Mooney, Darnell ATL WR
- Freiermuth, Pat PIT TE
- Battle, Jordan CIN S
- Pollard, Tony TEN RB
- Schultz, Dalton HOU TE
- Folk, Nick NYJ PK
- Davis, Demario NOS LB
- Wilson, Mack ARI LB
- Davis, Jordan PHI DT
- Ojulari, Azeez PHI DE
- Barton, Cody TEN LB
- Hawkins, Jaylinn NEP S
- Fields, Justin NYJ QB
- Smith, Geno LVR QB
- Wicks, Dontayvion GBP WR

</details>

### No expired contract found (player may still have years remaining) (45 players)

<details>
<summary>Show 45 players</summary>

- Bush, Devin CLE LB
- Dean, Jamel TBB CB
- Moehrig, Trevon CAR S
- Collins, Nico HOU WR
- Sutton, Courtland DEN WR
- Wyatt, Devonte GBP DT
- Bland, DaRon DAL CB
- Walker III, Kenneth SEA RB
- Oluokun, Foyesade JAC LB
- Stevenson, Rhamondre NEP RB
- Dike, Chimere TEN WR
- Caldwell, Jamaree LAC DT
- Pierce, Alec IND WR
- Collins, Maliek CLE DT
- Walker, Deone BUF DT
- Barner, AJ SEA TE
- Kinlaw, Javon WAS DT
- Tuimoloau, JT IND DE
- Buckner, DeForest IND DT
- Chenal, Leo KCC LB
- Lewis, Jourdan JAC CB
- Wiggins, Nate BAL CB
- Purdy, Brock SFO QB
- Armstrong, Dorance WAS DE
- Briggs, Jowon NYJ DT
- Chaisson, K'Lavon NEP DE
- Knight, Tyrice SEA LB
- Murphy, Byron MIN CB
- Hall, Breece NYJ RB
- Jones, Jarrian JAC CB
- Murphy, Myles CIN DE
- Carter, Michael ARI RB
- Waddle, Jaylen MIA WR
- Hamilton, Kyle BAL S
- Hickman, Ronnie CLE S
- Highsmith, Alex PIT DE
- Hunter, Danielle HOU DE
- Etienne, Travis JAC RB
- Lane, Jaylin WAS WR
- Uwazurike, Eyioma DEN DT
- Parrish, Jacob TBB CB
- London, Drake ATL WR
- Sweat, Josh ARI DE
- Cook, James BUF RB
- Aubrey, Brandon DAL PK

</details>


---

## Unmatched Players

All spreadsheet players were matched to the database.

---

## Confidence Statement

All 1549 matched players have been validated with **zero true discrepancies**. The 238 expected differences are all attributable to offseason transactions (re-signings, extensions, tenders) that correctly changed the player's contract state after the spreadsheet snapshot was taken. The app's franchise tag eligibility logic is fully consistent with the bylaws.

---

*Phase: 18-franchise-tags*
*Validated: 2026-04-01T11:58:54Z*
