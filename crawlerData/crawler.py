import json
import requests
import argparse
from bs4 import BeautifulSoup
from confluent_kafka import Producer


def add_contents(link, data):
    """
    Thu thập thông tin về bất động sản từ một liên kết và thêm vào danh sách dữ liệu.

    Parameters:
        link (str): Đường dẫn đến trang web chứa thông tin bất động sản.
        data (list): Danh sách dữ liệu để lưu trữ thông tin bất động sản.

    Returns:
        None
    """
    # Gửi yêu cầu GET đến URL và lấy nội dung HTML
    response = requests.get(link)

    # Kiểm tra xem yêu cầu có thành công không (status code 200 là thành công)
    if response.status_code == 200:
        # Lấy nội dung HTML từ phản hồi
        html = response.text

        # Tạo đối tượng BeautifulSoup để phân tích HTML
        soup = BeautifulSoup(html, 'html.parser')

        # Tìm thẻ li chứa thẻ a có href là "https://batdongsan.vn/"
        target_li = soup.find('li', string=lambda text: text and "Trang chủ" in text)
        if target_li:
            # Lấy danh sách các thẻ li ngay sau thẻ target_li
            next_li_tags = target_li.find_next_siblings('li', limit=3)
            loai = next_li_tags[0].text.strip()
            tinh = next_li_tags[1].text.strip()
            khuvuc = next_li_tags[2].text.strip()

            # Tìm tất cả các thẻ div có class="meta"
            meta_divs = soup.find_all("div", class_="meta")

            for div in meta_divs:
                strong_tag = div.find("strong", class_="price")
                if strong_tag:
                    gia = strong_tag.text.strip()

        # Tìm tất cả các thẻ <li> có nội dung mong muốn
        li_elements = soup.find_all("li")
        for li in li_elements:
            if "Diện tích:" in li.text:
                # Tìm và in ra số trong thẻ <li>
                dien_tich=''.join(filter(str.isdigit, li.text))

        # Tạo một dictionary
        property_info = {
            "Loại": loai,
            "Tỉnh": tinh,
            "Khu vực": khuvuc,
            "Giá tiền": gia,
            "Diện tích": dien_tich
        }
        data.append(property_info)
    else:
        print("Yêu cầu không thành công. Mã trạng thái:", response.status_code)


def get_list_link(start, end):
    """
    Tạo danh sách các liên kết dựa trên phạm vi được cung cấp.

    Parameters:
        start (int): Giá trị bắt đầu của phạm vi.
        end (int): Giá trị kết thúc của phạm vi.

    Returns:
        list: Danh sách các liên kết.
    """
    base_url = "https://batdongsan.vn/ban-dat-o-binh-phan-1000m2-cho-gao-tien-giang-gan-bo-song-r{}"
    links = []
    for i in range(start, end + 1):
        id = 285160 - i
        url = base_url.format(id)
        links.append(url)
    return links


def crawl_contents(producer, topic, links_home):
    """
    Thu thập nội dung từ các liên kết và đẩy vào Kafka topic.

    Parameters:
        producer (Producer): Đối tượng Producer của Apache Kafka.
        topic (str): Tên của Kafka topic.
        links_home (list): Danh sách các liên kết của các căn hộ cần thu thập thông tin.

    Returns:
        None
    """
    for link in links_home:
        try:
            # Tạo danh sách dữ liệu để lưu trữ thông tin bất động sản
            data = []
            add_contents(link, data)

            # Chuyển dữ liệu sang định dạng JSON
            json_data = json.dumps(data)

            # Gửi dữ liệu vào Kafka topic
            producer.produce(topic, value=json_data)

            print(f"Đã gửi dữ liệu từ {link} vào Kafka topic {topic} với dữ liệu:\n{json_data}")
        except requests.exceptions.RequestException as e:
            # Bắt các ngoại lệ và in thông báo lỗi
            print(f"Lỗi: Đã xảy ra ngoại lệ: {e}")
        except Exception as ex:
            # In thông báo lỗi nếu có lỗi xảy ra trong quá trình thu thập dữ liệu
            print(f"Lỗi: {ex}")


if __name__ == "__main__":
    print("Đang phân tích các đối số")
    parser = argparse.ArgumentParser()
    parser.add_argument("start", type=int)
    parser.add_argument("end", type=int)
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Bootstrap server(s) của Kafka")
    parser.add_argument("--topic", type=str, default="data", help="Tên của Kafka topic")
    args = parser.parse_args()

    print("Bắt đầu thu thập từ", args.start, "đến", args.end)
    links = get_list_link(args.start, args.end)
    print("Danh sách các liên kết:")
    print(links)

    # Tạo một producer Kafka
    producer_conf = {'bootstrap.servers': args.bootstrap_servers}
    producer = Producer(producer_conf)

    # Thu thập dữ liệu từ các liên kết và gửi vào Kafka topic
    crawl_contents(producer, args.topic, links)

    # Flush để đảm bảo tất cả các tin nhắn đã được gửi đi
    producer.flush()
