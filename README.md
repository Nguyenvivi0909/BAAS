# ỨNG DỤNG KAFKA KẾT HỢP SPARK

## 1. Dữ liệu
Tập dữ liệu sử dụng là dữ liệu chứng khoán lịch sử của Công ty cổ phần Tập đoàn Hòa Phát (HPG). Tập dữ liệu được lấy từ ngày 16/11/2007 đến ngày 16/10/2023 và được lấy trực tiếp từ link: investing.com. Tập dữ liệu gồm 3995 hàng và 7 trường bao gồm:
-	"Date" (Ngày): Đây là trường dữ liệu chứa thông tin về ngày mà dữ liệu được thu thập hoặc ghi nhận.
-	"Price" (Giá): Trường này chứa thông tin về giá đóng cửa của tài sản tài chính (ví dụ: cổ phiếu) tại ngày tương ứng.
-	"Open" (Giá mở cửa): Đây là giá tài sản tài chính tại thời điểm mở cửa phiên giao dịch. Nó là giá đầu tiên của phiên giao dịch đó.
-	"High" (Giá cao nhất): Trường này cho biết giá cao nhất mà tài sản tài chính đã đạt được trong suốt phiên giao dịch đó.
-	"Low" (Giá thấp nhất): Đây là giá thấp nhất mà tài sản tài chính đã đạt được trong suốt phiên giao dịch đó.
-	"Vol." (Khối lượng giao dịch): Trường này chứa thông tin về khối lượng giao dịch, tức là số lượng tài sản tài chính đã được giao dịch trong phiên giao dịch tương ứng.
-	"Change %" (Tỷ lệ thay đổi): Trường này cho biết tỷ lệ thay đổi giữa giá đóng cửa tại ngày đó so với ngày giao dịch trước đó dưới dạng phần trăm.2. 

## 2. Sơ đồ ứng dụng
![image](https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcT7boGebeZXJH1siVbX7Rs24Wks4g06IzN2V4l2xr6FBD3k60Zf)
