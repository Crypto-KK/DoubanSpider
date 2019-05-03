
import jieba.analyse
# from wordcloud import WordCloud
# import matplotlib.pyplot as plt

from models import create_session, Comments

session = create_session()

queryset = session.query(Comments).all()

text = ''
for i in queryset:
    text += i.content

text = text.replace('\n', '').replace('\r', '')

topWords = jieba.analyse.extract_tags(text, topK=40)
print(topWords)

# w = WordCloud(background_color='white', width=800, height=600, margin=2).generate(topWords)

# plt.imshow(w)
# plt.axis('off')
# plt.show()
